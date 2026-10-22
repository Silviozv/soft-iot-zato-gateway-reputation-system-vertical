# -*- coding: utf-8 -*-

import threading
import json
import os
import logging

from soft_iot_id_manager import IDManager
from soft_iot_gateway_state import GatewayStateManager
from soft_iot_node_type import NodeTypeManager

import zmq.green as zmq

from zato.server.service import Service

logger = logging.getLogger('zato.zmq') 

CONFIG_FILE_PATH = '/home/ubuntu/mapping_archives/devices_config/devices.json'

class ZMQManager:
    """
    Controlador Singleton para gerenciar a conexão ZMQ de forma compatível com Zato.
    Atua como um Dispatcher rápido: Lê da rede e despacha para os serviços em background.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        logger.info("Criando instância do ZMQManager...")
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ZMQManager, cls).__new__(cls)
                    cls._instance.is_running = False
                    cls._instance.context = zmq.Context()
                    cls._instance.socket = None
                    cls._instance.server = None 

        return cls._instance 

    def _init_configs(self):
        self.zmq_ip = os.environ.get('Zato_ZMQ_IP', '127.0.0.1')
        self.zmq_port = os.environ.get('Zato_ZMQ_PORT', '5556')
        self.zmq_url = f"tcp://{self.zmq_ip}:{self.zmq_port}"

    def start(self):
        if self.is_running:
            return
        
        if not hasattr(self, 'zmq_url'):
            self._init_configs()
            
        logger.info(f"Iniciando ZMQ Server Thread conectando em: {self.zmq_url}")
    
        self.is_running = True

        self.thread = threading.Thread(target=self._run, name="ZMQServerThread")
        self.thread.daemon = True
        self.thread.start()

        logger.info(f"Start finalizado")
        return
        

    def _run(self):
        try:
            self.socket = self.context.socket(zmq.SUB)
            self.socket.connect(self.zmq_url)
            self.socket.setsockopt_string(zmq.SUBSCRIBE, "REP_") 
            
            logger.info(f"ZMQ conectado com sucesso em {self.zmq_url}")

            while self.is_running:
                logger.info(f"Iniciando leitura (Modo Green/Assíncrono)")

                frames = self.socket.recv_multipart()

                for frame in frames:
                    raw_content = frame.decode('utf-8')
                    parts = raw_content.split(' ', 1)

                    logger.info(f"[ZMQ RAW DUMP] Tráfego intercetado: {raw_content}")

                    if len(parts) < 2:
                        continue

                    topic_str = parts[0]
                    json_payload_str = parts[1]

                    try:
                        full_payload = json.loads(json_payload_str)
                        inner_data_str = full_payload.get('payload', {}).get('data', '{}')
                        app_data = json.loads(inner_data_str)

                        msg_type = app_data.get('type')

                        logger.info("MENSAGEM ENCONTRADA")

                        if topic_str == "REP_HAS_SVC":
                            logger.info("MENSAGEM VÁLIDA ENCONTRADA")
                            if msg_type == 'REP_SVC_RES' and self.server:
                                self.server.invoke_async('soft-iot.zmq.handler.response', app_data, None)
                            elif msg_type == 'REP_SVC_REQ' and self.server:
                                self.server.invoke_async('soft-iot.zmq.handler.request', app_data, None)
                    
                    except json.JSONDecodeError:
                        logger.error(f"Erro ao decodificar JSON do tópico {topic_str}")

        except Exception as e:
            logger.error(f"Erro crítico no loop ZMQ: {e}")
            self.is_running = False


class ZMQResponseHandlerService(Service):
    """
    Serviço Zato dedicado a interceptar respostas de requisições de serviço (REP_SVC_RES).
    Rodará numa Thread segura gerida pelo Zato.
    """
    name = 'soft-iot.zmq.handler.response'

    def handle(self):
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


class ZMQRequestHandlerService(Service):
    """
    Serviço Zato dedicado a interceptar requisições (REP_SVC_REQ).
    """
    name = 'soft-iot.zmq.handler.request'

    def _load_devices_from_file(self):
        try:
            if not os.path.exists(CONFIG_FILE_PATH):
                logger.warning(f"Arquivo de dispositivos não encontrado em: {CONFIG_FILE_PATH}")
                return []

            with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data
        except Exception as e:
            logger.error(f"Erro ao ler devices.json: {e}")
            return []

    def handle(self):
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

        matched_services = []

        if current_conduct == "MALICIOUS":
            logger.warning(f"Conduta MALICIOSA ativa: Ocultando sensores reais e publicando falsos para '{requested_service}'.")
            
            # Adiciona estritamente o dispositivo e sensor inexistentes (Spoofing)
            matched_services.append({
                "device_id": "nonexistentDevice",
                "sensor_id": "nonexistentSensor"
            })

        else:
            # Conduta HONEST, SELFISH ou Perturbador antes da mudança
            logger.info("Conduta regular ativa: Buscando sensores reais no arquivo local.")
            devices = self._load_devices_from_file()

            # Buscando sensores reais com serviço requisitado
            for device in devices:
                device_id = device.get('id')
                sensors = device.get('sensors', [])
                
                for sensor in sensors:
                    if sensor.get('type') == requested_service:
                        matched_services.append({
                            "device_id": device_id,
                            "sensor_id": sensor.get('id')
                        })
        
        if matched_services:
            logger.info(f"Encontrados {len(matched_services)} sensores compatíveis. Preparando REP_SVC_RES.")
            
            id_manager = IDManager()
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
                "services": matched_services
            }

            # Comunicação nativa via memória (Zero HTTP Overhead)
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


class ZMQStartupService(Service):
    """
    Serviço responsável por ativar o Singleton do ZMQ Manager.
    """
    name = 'soft-iot.zmq.start'

    def handle(self):
        logger.info("Recebida requisição para iniciar ZMQ Manager.")
        manager = ZMQManager()
        
        manager.server = self.server 
        
        logger.info(f"ZMQ Manager status atual: {'operando' if manager.is_running else 'parado'}")
        
        if not manager.is_running:
            manager.start()
            logger.info("Comando de startup ZMQ enviado com sucesso.")
        else:
            logger.info("ZMQ Manager já está operando.")

        self.response.payload = {"status": "success", "message": "Startup concluído"}