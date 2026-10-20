# -*- coding: utf-8 -*-
"""
soft_iot_common.py

Base compartilhada por todos os módulos. NÃO define serviços Zato e NÃO importa
nada do Zato, para poder ser testado fora do container.

Conteúdo:
  - constantes (caminhos, índices da DLT, tipos de mensagem, status, nomes de serviço)
  - leitura de variáveis de ambiente
  - IDManager (identidade do gateway)
  - acesso único ao devices.json
  - helpers do envelope de mensagens da DLT (montar e interpretar)
"""

import json
import logging
import os
import socket
import threading
import uuid
import time

logger = logging.getLogger('zato.common')

# ---------------------------------------------------------------------------
# Caminhos e portas
# ---------------------------------------------------------------------------
DB_FILENAME = "/opt/zato/env/soft_iot_data.db"          # leituras dos sensores
GATEWAY_STATE_DB = "/tmp/gateway_state.db"              # estado local do gateway
DEVICES_FILE = "/home/ubuntu/mapping_archives/devices_config/devices.json"
GATEWAY_HTTP_PORT = 11223                               # porta REST do Zato


# ---------------------------------------------------------------------------
# DLT: índices e tipos de mensagem
# ---------------------------------------------------------------------------
INDEX_DISCOVERY = "REP_HAS_SVC"
CRED_INDEX_PREFIX = "CRED_"
ZMQ_SUBSCRIBE_PREFIX = "REP_"


def credibility_index(evaluator_id):
    return f"{CRED_INDEX_PREFIX}{evaluator_id}"


class MsgType:
    SVC_REQ = "REP_SVC_REQ"
    SVC_RES = "REP_SVC_RES"
    EVALUATION = "REP_EVALUATION"
    CRED_UPDATE = "CRED_UPDATE"


# ---------------------------------------------------------------------------
# Máquina de estados das requisições
# (CHOSING_BETTER_NODE foi corrigido para CHOOSING_BETTER_NODE)
# ---------------------------------------------------------------------------
class RequestStatus:
    WAITING_RESPONSES = "WAITING_RESPONSES"
    CHOOSING_BETTER_NODE = "CHOOSING_BETTER_NODE"
    REQUESTING_SERVICE = "REQUESTING_SERVICE"
    FINISHED = "FINISHED"
    FAILED = "FAILED"
    FAILED_NO_SERVICES = "FAILED_NO_SERVICES"
    FAILED_DATA_CONSUMPTION = "FAILED_DATA_CONSUMPTION"
    TIMEOUT = "TIMEOUT"

    IN_PROGRESS = (WAITING_RESPONSES, CHOOSING_BETTER_NODE, REQUESTING_SERVICE)


# ---------------------------------------------------------------------------
# Condutas e tipos de nó
# ---------------------------------------------------------------------------
class Conduct:
    HONEST = "HONEST"
    MALICIOUS = "MALICIOUS"
    SELFISH = "SELFISH"


class NodeType:
    HONEST = 1
    MALICIOUS = 2
    SELFISH = 3
    DISTURBING = 4


# ---------------------------------------------------------------------------
# Nomes de serviços Zato referenciados entre módulos
# ---------------------------------------------------------------------------
class ServiceName:
    ZMQ_HANDLER_REQUEST = 'soft-iot.zmq.handler.request'
    ZMQ_HANDLER_RESPONSE = 'soft-iot.zmq.handler.response'
    TASK_WAIT_RESPONSES = 'soft-iot.reputation.task.wait_nodes_responses'
    TASK_SELECT_BEST_NODE = 'soft-iot.reputation.task.select_best_node'
    TASK_REQUEST_NODE_SERVICE = 'soft-iot.reputation.task.request_node_service'


# Serviços que um gateway pode requisitar (sorteio a cada ciclo)
AVAILABLE_SERVICES = [
    "Thermometer",
    "HumiditySensor",
    "PulseOxymeter",
    "WindDirectionSensor",
]


# ---------------------------------------------------------------------------
# Variáveis de ambiente
# ---------------------------------------------------------------------------
def env_str(name, default=None):
    return os.environ.get(name, default)


def env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Identidade do gateway (singleton)
# ---------------------------------------------------------------------------
class IDManager:
    """UUID, IP e grupo do gateway. Substitui o serviço soft-iot.id.manager."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    inst = super(IDManager, cls).__new__(cls)
                    inst._id = str(uuid.uuid4())
                    inst._ip = inst._get_real_ip()
                    inst._group = env_str('Zato_GROUP', 'cloud/c1')
                    cls._instance = inst
                    logger.info(f"ID Manager iniciado - ID: {inst._id} - IP: {inst._ip}")
        return cls._instance

    @staticmethod
    def _get_real_ip():
        ip = os.environ.get('Zato_GATEWAY_REAL_IP')
        if not ip:
            try:
                ip = socket.gethostbyname(socket.gethostname())
            except Exception:
                ip = '127.0.0.1'
        return ip.strip()

    @property
    def id(self):
        return self._id

    @property
    def ip(self):
        return self._ip

    @property
    def group(self):
        return self._group


# ---------------------------------------------------------------------------
# devices.json (único ponto de leitura e escrita)
# ---------------------------------------------------------------------------
def load_devices():
    try:
        if not os.path.exists(DEVICES_FILE):
            logger.warning(f"Arquivo de dispositivos não encontrado em: {DEVICES_FILE}")
            return []
        with open(DEVICES_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (OSError, ValueError) as e:
        logger.error(f"Erro ao ler devices.json: {e}")
        return []


def save_devices(devices):
    try:
        os.makedirs(os.path.dirname(DEVICES_FILE), exist_ok=True)
        with open(DEVICES_FILE, 'w', encoding='utf-8') as f:
            json.dump(devices, f, indent=4)
        return True
    except OSError as e:
        logger.error(f"Erro ao salvar arquivo de dispositivos: {e}")
        return False


def find_device(device_id):
    return next((d for d in load_devices() if d.get('id') == device_id), None)


def find_sensors_by_type(service_type):
    """Lista [{device_id, sensor_id}] dos sensores cujo `type` é o serviço pedido."""
    matched = []
    for device in load_devices():
        for sensor in device.get('sensors', []):
            if sensor.get('type') == service_type:
                matched.append({"device_id": device.get('id'), "sensor_id": sensor.get('id')})
    return matched


# ---------------------------------------------------------------------------
# Envelope de mensagens da DLT
# ---------------------------------------------------------------------------
def decode_data(item):
    """
    O campo `data` de uma mensagem lida da DLT pode vir como string JSON ou dict.
    Retorna o dict da aplicação, ou None se não for interpretável.
    """
    data = item.get('data') if isinstance(item, dict) else None
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except (ValueError, TypeError):
            return None
    return data if isinstance(data, dict) else None


def filter_by_type(items, msg_type):
    """Dos itens lidos da DLT, devolve os dicts de aplicação com `type == msg_type`."""
    result = []
    for item in items or []:
        data = decode_data(item)
        if data and data.get('type') == msg_type:
            result.append(data)
    return result


def _timestamp(msg):
    try:
        return int(msg.get('timestamp', 0) or 0)
    except (ValueError, TypeError):
        return 0


def sort_newest_first(messages):
    return sorted(messages, key=_timestamp, reverse=True)


def now_ts():
    return int(time.time())


def build_service_request(source, group, requested_service, id_request):
    return {
        "source": source,
        "group": group,
        "type": MsgType.SVC_REQ,
        "requestedService": requested_service,
        "idRequest": id_request,
    }


def build_service_response(target, id_request, source, ip_source, group, services):
    return {
        "type": MsgType.SVC_RES,
        "target": target,
        "idRequest": id_request,
        "source": source,
        "ipSource": ip_source,
        "group": group,
        "services": services,
    }


def build_evaluation(source, group, target, service_evaluation, node_credibility, value):
    return {
        "source": source,
        "group": group,
        "type": MsgType.EVALUATION,
        "target": target,
        "serviceEvaluation": service_evaluation,
        "nodeCredibility": float(node_credibility),
        "value": value,
        "timestamp": now_ts(),
    }


def build_credibility_update(evaluator_id, credibility):
    return {
        "type": MsgType.CRED_UPDATE,
        "evaluator_id": evaluator_id,
        "credibility": float(credibility),
        "timestamp": now_ts(),
    }