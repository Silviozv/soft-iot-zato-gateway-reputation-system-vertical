# -*- coding: utf-8 -*-

"""
Escuta em tempo real da Tangle direto pelo broker MQTT do nó Hornet (substitui a ponte ZMQ).

O Hornet (IOTA 1.5 / Chrysalis) publica cada mensagem indexada no tópico
'messages/indexation/{index em hexadecimal}', com a mensagem serializada em binário.
Este módulo assina os índices de interesse, extrai o campo 'data' da mensagem e despacha
para os tratadores de requisição e resposta de serviço.
"""

import os
import json
import uuid
import struct
import sqlite3
import logging
import threading

import paho.mqtt.client as mqtt

from zato.server.service import Service

# Os módulos soft_iot_id_manager, soft_iot_gateway_state e soft_iot_node_type são importados
# dentro dos handle(): o Zato carrega os arquivos em ordem alfabética e este vem antes deles.


logger = logging.getLogger('zato.dlt.mqtt')

CONFIG_FILE_PATH = '/home/ubuntu/mapping_archives/devices_config/devices.json'
DATA_DB_FILENAME = '/opt/zato/env/soft_iot_data.db'  # Mesmo banco do armazenamento local

# Tópico do Hornet para mensagens indexadas (o índice vai em hexadecimal)
HORNET_INDEXATION_TOPIC = "messages/indexation/{index}"

# Tipo do payload de indexação na Chrysalis
INDEXATION_PAYLOAD_TYPE = 2


def parse_indexation_message(raw):
    """
    Lê uma mensagem binária da Chrysalis (iota.go v2) e retorna (index, data) como texto.
    Retorna None se a mensagem não tiver payload de indexação ou estiver malformada.

    Formato (little-endian):
      networkId u64 | qtd. de pais u8 | pais (32 bytes cada) | tamanho do payload u32 |
      payload [tipo u32 | tamanho do índice u16 | índice | tamanho dos dados u32 | dados] | nonce u64
    """
    try:
        offset = 8  # networkId

        parents_count = raw[offset]
        offset += 1 + (parents_count * 32)

        (payload_length,) = struct.unpack_from('<I', raw, offset)
        offset += 4

        if payload_length == 0:
            return None

        (payload_type,) = struct.unpack_from('<I', raw, offset)
        if payload_type != INDEXATION_PAYLOAD_TYPE:
            return None
        offset += 4

        (index_length,) = struct.unpack_from('<H', raw, offset)
        offset += 2
        index = raw[offset:offset + index_length]
        offset += index_length

        (data_length,) = struct.unpack_from('<I', raw, offset)
        offset += 4
        data = raw[offset:offset + data_length]

        if len(index) != index_length or len(data) != data_length:
            return None

        return index.decode('utf-8'), data.decode('utf-8')

    except (IndexError, struct.error, UnicodeDecodeError):
        return None


class DLTMQTTManager:
    """
    Controlador Singleton da conexão MQTT com o nó Hornet.
    Atua como dispatcher: recebe as mensagens indexadas e despacha para os serviços Zato.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DLTMQTTManager, cls).__new__(cls)
                    cls._instance.is_running = False
                    cls._instance.client = None
                    cls._instance.server = None

        return cls._instance

    def _init_configs(self):
        self.mqtt_ip = os.environ.get('Zato_DLT_MQTT_IP', '127.0.0.1')
        self.mqtt_port = int(os.environ.get('Zato_DLT_MQTT_PORT', '1883'))

        # Índices assinados (separados por vírgula). Só o REP_HAS_SVC é usado em tempo real.
        indexes = os.environ.get('Zato_DLT_MQTT_INDEXES', 'REP_HAS_SVC')
        self.indexes = [index.strip() for index in indexes.split(',') if index.strip()]

        self.topics = [
            HORNET_INDEXATION_TOPIC.replace('{index}', index.encode('utf-8').hex())
            for index in self.indexes
        ]

    def start(self):
        if self.is_running:
            return

        self._init_configs()

        logger.info(f"Iniciando conexão MQTT com o Hornet em {self.mqtt_ip}:{self.mqtt_port} | Índices: {self.indexes}")

        # O client_id precisa ser único no broker do Hornet: se dois gateways usarem o mesmo,
        # o broker derruba um ao conectar o outro (loop de desconexões e mensagens perdidas).
        self.client = mqtt.Client(client_id=f"SoftIoT_DLT_{uuid.uuid4().hex}")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)

        # connect_async + loop_start: o paho tenta conectar e reconectar sozinho
        self.client.connect_async(self.mqtt_ip, self.mqtt_port, 60)
        self.client.loop_start()

        self.is_running = True
        logger.info("Start finalizado")

    def _on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            logger.error(f"Falha ao conectar no MQTT do Hornet (código {rc}).")
            return

        # Assina novamente a cada (re)conexão
        client.subscribe([(topic, 2) for topic in self.topics])
        logger.info(f"MQTT do Hornet conectado. Tópicos assinados: {self.topics}")

    def _on_disconnect(self, client, userdata, rc):
        logger.warning(f"MQTT do Hornet desconectado (código {rc}). O paho tentará reconectar.")

    def _on_message(self, client, userdata, msg):
        try:
            parsed = parse_indexation_message(msg.payload)
            if parsed is None:
                logger.warning(f"Mensagem ignorada no tópico {msg.topic}: não é uma indexação válida.")
                return

            index, data = parsed
            logger.info(f"[DLT MQTT] Mensagem recebida | índice: {index} | dados: {data}")

            app_data = json.loads(data)
            msg_type = app_data.get('type')

            if index == "REP_HAS_SVC" and self.server:
                if msg_type == 'REP_SVC_RES':
                    self.server.invoke_async('soft-iot.dlt.handler.response', app_data, None)
                elif msg_type == 'REP_SVC_REQ':
                    self.server.invoke_async('soft-iot.dlt.handler.request', app_data, None)

        except json.JSONDecodeError:
            logger.error(f"Erro ao decodificar o JSON da mensagem do tópico {msg.topic}")
        except Exception as e:
            logger.error(f"Erro ao processar mensagem MQTT do Hornet: {e}")


class DLTResponseHandlerService(Service):
    """
    Serviço Zato dedicado a interceptar respostas de requisições de serviço (REP_SVC_RES).
    """
    name = 'soft-iot.dlt.handler.response'

    def handle(self):
        from soft_iot_id_manager import IDManager
        from soft_iot_gateway_state import GatewayStateManager

        payload = self.request.payload

        id_manager = IDManager()
        target = payload.get('target')

        # Verifica se o gateway atual é o alvo
        if target != id_manager.id:
            logger.info("NÃO É O ALVO")
            return

        logger.info("GRAVANDO")

        # Extração dos dados
        id_request = payload.get('idRequest')
        source = payload.get('source')
        ip_source = payload.get('ipSource')
        services = payload.get('services')
        group_name = payload.get('group')

        gs_manager = GatewayStateManager()

        # Filtro: Verifica o status da máquina de estados
        current_status = gs_manager.get_request_status(id_request)

        if current_status == 'WAITING_RESPONSES':
            logger.info(f"Recebida proposta válida do provedor {source} (IP: {ip_source}).")
            gs_manager.save_response(id_request, source, ip_source, target, services, group_name)


class DLTRequestHandlerService(Service):
    """
    Serviço Zato dedicado a interceptar requisições (REP_SVC_REQ).
    """
    name = 'soft-iot.dlt.handler.request'

    def _load_devices_from_file(self):
        try:
            if not os.path.exists(CONFIG_FILE_PATH):
                logger.warning(f"Arquivo de dispositivos não encontrado em: {CONFIG_FILE_PATH}")
                return []

            with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
                content = f.read()

            # Arquivo vazio: nenhum device conectou ainda
            if not content.strip():
                return []

            return json.loads(content)
        except Exception as e:
            logger.error(f"Erro ao ler devices.json: {e}")
            return []

    def _has_stored_data(self, device_id, sensor_id):
        """
        Verifica se já existe algum dado armazenado para o par device/sensor.
        Evita anunciar um serviço de um device que ainda não enviou dados.
        """
        # Com a persistência desligada não há como verificar; mantém o comportamento anterior
        if os.getenv('Zato_SAVE_DATA_ENABLED', 'True') == 'False':
            return True

        try:
            if not os.path.exists(DATA_DB_FILENAME):
                return False

            conn = sqlite3.connect(DATA_DB_FILENAME, timeout=5)
            try:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT 1 FROM sensor_data WHERE device_id = ? AND sensor_id = ? LIMIT 1',
                    (device_id, sensor_id)
                )
                return cursor.fetchone() is not None
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Erro ao verificar dados de {device_id}/{sensor_id}: {e}")
            return False

    def handle(self):
        from soft_iot_id_manager import IDManager
        from soft_iot_gateway_state import GatewayStateManager
        from soft_iot_node_type import NodeTypeManager

        payload = self.request.payload

        requested_service = payload.get('requestedService')
        requester_id = payload.get('source')
        id_request = payload.get('idRequest')

        if not requested_service or not requester_id:
            logger.warning("Requisição recebida com parâmetros nulos.")
            return

        id_manager = IDManager()

        if requester_id == id_manager.id:
            logger.info("ELE MESMO MANDOU")
            return

        logger.info(f"Processando requisição de serviço '{requested_service}' do nó {requester_id}.")

        nt_manager = NodeTypeManager()
        gs_manager = GatewayStateManager()

        # Sincroniza a conduta atual baseada no estado do gateway
        behavior_changed = gs_manager.get_behavior_changed()
        nt_manager.define_conduct(behavior_changed)
        current_conduct = nt_manager.current_conduct

        # Egoísta omite que possui o serviço e não recebe requisições diretas
        if current_conduct == "SELFISH":
            logger.info(f"Conduta EGOÍSTA ativa: omitindo resposta para o serviço '{requested_service}'.")
            return

        # O gateway apenas verifica se algum device conectado possui o serviço.
        # A escolha do device é feita pelo requisitor, consultando a Tangle.
        # A conduta maliciosa de provedor é aplicada somente na requisição direta.
        devices = self._load_devices_from_file()

        matched_services = []
        for device in devices:
            device_id = device.get('id')
            sensors = device.get('sensors', [])

            for sensor in sensors:
                if sensor.get('type') == requested_service and self._has_stored_data(device_id, sensor.get('id')):
                    matched_services.append({
                        "device_id": device_id,
                        "sensor_id": sensor.get('id')
                    })

        if matched_services:
            logger.info(f"Encontrados {len(matched_services)} sensores compatíveis. Preparando REP_SVC_RES.")

            my_gateway_id = id_manager.id
            my_group = id_manager.group
            my_ip = id_manager.ip

            response_tx = {
                "type": "REP_SVC_RES",
                "target": requester_id,
                "idRequest": id_request,
                "source": my_gateway_id,
                "ipSource": my_ip,
                "group": my_group,
                "requestedService": requested_service,
                "hasService": True,
                "services": []  # Os devices não são listados; o requisitor os consulta na Tangle
            }

            try:
                dlt_payload = {
                    "index": "REP_HAS_SVC",
                    "data": response_tx
                }

                # Despacha o envio para a Tangle de forma assíncrona
                self.invoke_async('soft-iot.dlt.client.api.write', dlt_payload)
                logger.info(f"Oferta de serviço encaminhada para gravação na Tangle (Nó {requester_id}).")

            except Exception as e:
                logger.error(f"Erro interno ao acionar a API local de DLT: {e}")

        else:
            logger.info(f"O nó local não possui o serviço '{requested_service}'. Ignorando.")


class DLTMQTTStartupService(Service):
    """
    Serviço responsável por ativar o Singleton da conexão MQTT com o Hornet.
    """
    name = 'soft-iot.dlt.mqtt.start'

    def handle(self):
        logger.info("Recebida requisição para iniciar a conexão MQTT com o Hornet.")
        manager = DLTMQTTManager()

        manager.server = self.server

        if not manager.is_running:
            manager.start()
            logger.info("Conexão MQTT com o Hornet iniciada.")
        else:
            logger.info("Conexão MQTT com o Hornet já está operando.")

        self.response.payload = {"status": "success", "message": "Startup concluído"}
