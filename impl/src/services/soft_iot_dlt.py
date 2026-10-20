# -*- coding: utf-8 -*-
"""
soft_iot_dlt.py

Os dois canais com a DLT, sem regra de negócio:
  - REST (Tangle Hornet API): write_message() / read_index()
  - ZMQ (ponte): ZMQManager, que assina REP_ e despacha para os tratadores

Serviços Zato:
  soft-iot.dlt.client.api.write       (também usado via invoke_async)
  soft-iot.dlt.client.api.read_index
  soft-iot.zmq.start
"""

import json
import logging
import threading

import requests
import zmq.green as zmq

from zato.server.service import Service

from soft_iot_common import (
    INDEX_DISCOVERY, MsgType, ServiceName, ZMQ_SUBSCRIBE_PREFIX, env_str,
)

logger = logging.getLogger('zato.dlt')
zmq_logger = logging.getLogger('zato.zmq')

HTTP_TIMEOUT = 10


# ===========================================================================
# Canal REST
# ===========================================================================
def _base_url():
    ip = env_str('Zato_TANGLE_API_IP', '127.0.0.1')
    port = env_str('Zato_TANGLE_API_PORT', '3000')
    return f"http://{ip}:{port}"


def write_message(index, data):
    """
    POST /message {index, data}. Nunca levanta exceção.
    Retorna {"status": "success", "tangle_res": ...} ou {"status": "error", ...}.
    """
    if not index or not data:
        logger.error("Index ou data ausentes no payload.")
        return {"status": "error", "message": "Missing index or data"}

    url = f"{_base_url()}/message"
    try:
        response = requests.post(url, json={'index': index, 'data': data}, timeout=HTTP_TIMEOUT)

        if response.status_code in (200, 201):
            logger.info(f"RESPOSTA DA ESCRITA: {response.text}")
            if response.text and response.text.strip():
                try:
                    return {"status": "success", "tangle_res": response.json()}
                except ValueError:
                    return {"status": "error"}
            return {"status": "error"}

        logger.error(f"Erro na API Hornet ({response.status_code}): {response.text}")
        return {"status": "error", "code": response.status_code}

    except Exception as e:
        logger.error(f"Falha na requisição para {url}: {e}")
        return {"status": "error", "message": str(e)}


def read_index(index):
    """
    GET /message/{index}. Sempre devolve uma lista (vazia em qualquer falha).
    O campo `data` que vier como string JSON é convertido em dict quando possível.
    """
    if not index:
        logger.error("Índice não fornecido para busca.")
        return []

    url = f"{_base_url()}/message/{index}"
    try:
        response = requests.get(url, timeout=HTTP_TIMEOUT)
        if response.status_code != 200 or not response.text.strip():
            return []

        try:
            raw_data = response.json()
        except ValueError:
            return []
        if not isinstance(raw_data, list):
            return []

        for item in raw_data:
            if isinstance(item, dict) and isinstance(item.get('data'), str):
                try:
                    item['data'] = json.loads(item['data'])
                except json.JSONDecodeError:
                    pass
        return raw_data

    except Exception as e:
        logger.error(f"Falha na leitura do índice {index}: {e}")
        return []


class DLTWriterService(Service):
    """Escrita de mensagem indexada na Tangle."""
    name = 'soft-iot.dlt.client.api.write'

    def handle(self):
        payload = self.request.payload or {}
        self.response.payload = write_message(payload.get('index'), payload.get('data'))


class DLTIndexReaderService(Service):
    """Leitura das mensagens de um índice."""
    name = 'soft-iot.dlt.client.api.read_index'

    def handle(self):
        request_data = self.request.payload or self.request.params or {}
        self.response.payload = read_index(request_data.get('index'))


# ===========================================================================
# Canal ZMQ
# ===========================================================================
class ZMQManager:
    """
    Singleton: assina a ponte ZMQ em uma thread daemon e despacha cada mensagem
    para o tratador Zato correspondente (invoke_async). Não decide nada.
    """

    _instance = None
    _lock = threading.Lock()

    # (tópico, type) -> serviço que trata a mensagem
    ROUTES = {
        (INDEX_DISCOVERY, MsgType.SVC_RES): ServiceName.ZMQ_HANDLER_RESPONSE,
        (INDEX_DISCOVERY, MsgType.SVC_REQ): ServiceName.ZMQ_HANDLER_REQUEST,
    }

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    inst = super(ZMQManager, cls).__new__(cls)
                    inst.is_running = False
                    inst.context = zmq.Context()
                    inst.socket = None
                    inst.server = None
                    cls._instance = inst
        return cls._instance

    def _init_configs(self):
        ip = env_str('Zato_ZMQ_IP', '127.0.0.1')
        port = env_str('Zato_ZMQ_PORT', '5556')
        self.zmq_url = f"tcp://{ip}:{port}"

    def start(self):
        if self.is_running:
            return
        if not hasattr(self, 'zmq_url'):
            self._init_configs()

        zmq_logger.info(f"Iniciando ZMQ Server Thread conectando em: {self.zmq_url}")
        self.is_running = True
        self.thread = threading.Thread(target=self._run, name="ZMQServerThread")
        self.thread.daemon = True
        self.thread.start()

    def _dispatch(self, raw_content):
        parts = raw_content.split(' ', 1)
        if len(parts) < 2:
            return

        topic, json_payload = parts
        try:
            full_payload = json.loads(json_payload)
            app_data = json.loads(full_payload.get('payload', {}).get('data', '{}'))
        except (ValueError, TypeError, AttributeError):
            zmq_logger.error(f"Erro ao decodificar mensagem do tópico {topic}")
            return

        service = self.ROUTES.get((topic, app_data.get('type')))
        if service and self.server:
            self.server.invoke_async(service, app_data, None)

    def _run(self):
        try:
            self.socket = self.context.socket(zmq.SUB)
            self.socket.connect(self.zmq_url)
            self.socket.setsockopt_string(zmq.SUBSCRIBE, ZMQ_SUBSCRIBE_PREFIX)
            zmq_logger.info(f"ZMQ conectado com sucesso em {self.zmq_url}")

            while self.is_running:
                for frame in self.socket.recv_multipart():
                    raw_content = frame.decode('utf-8')
                    zmq_logger.debug(f"[ZMQ RAW] {raw_content}")
                    self._dispatch(raw_content)

        except Exception as e:
            zmq_logger.error(f"Erro crítico no loop ZMQ: {e}")
            self.is_running = False


def start_zmq(server):
    """Liga o ZMQ Manager (idempotente). Usado pelo bootstrap e pelo serviço zmq.start."""
    manager = ZMQManager()
    manager.server = server
    if manager.is_running:
        zmq_logger.info("ZMQ Manager já está operando.")
    else:
        manager.start()
        zmq_logger.info("Comando de startup ZMQ enviado com sucesso.")
    return manager


class ZMQStartupService(Service):
    name = 'soft-iot.zmq.start'

    def handle(self):
        start_zmq(self.server)
        self.response.payload = {"status": "success", "message": "Startup concluído"}