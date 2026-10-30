# -*- coding: utf-8 -*-

import socket
import uuid
import os
import threading
import logging
import time

from zato.server.service import Service


logger = logging.getLogger('zato.id.manager')

# Arquivo que guarda o ID do gateway dentro do container. Garante o mesmo ID mesmo quando o
# Zato carrega mais de uma cópia deste módulo (cada cópia teria o seu próprio singleton).
GATEWAY_ID_FILE = '/tmp/gateway_id'


def _load_or_create_gateway_id():
    """
    Lê o ID salvo; se não existir, gera um novo e grava de forma atômica.
    Se duas cópias tentarem criar ao mesmo tempo, só uma grava e a outra lê o valor gravado.
    """
    try:
        new_id = str(uuid.uuid4())
        fd = os.open(GATEWAY_ID_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        with os.fdopen(fd, 'w') as f:
            f.write(new_id)
        return new_id
    except FileExistsError:
        pass
    except Exception as e:
        logger.error(f"Não foi possível gravar o ID do gateway em {GATEWAY_ID_FILE}: {e}")
        return str(uuid.uuid4())

    # O arquivo já existe: aguarda a outra cópia terminar de gravar e lê o ID
    for _ in range(50):
        try:
            with open(GATEWAY_ID_FILE, 'r') as f:
                saved_id = f.read().strip()
            if saved_id:
                return saved_id
        except Exception:
            pass
        time.sleep(0.01)

    logger.error(f"Arquivo {GATEWAY_ID_FILE} vazio ou ilegível. Gerando ID temporário.")
    return str(uuid.uuid4())


class IDManager:

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """"
        Padrão Singleton para o identificador único do gateway.
        """

        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(IDManager, cls).__new__(cls)
                    cls._instance._id = _load_or_create_gateway_id()     # ID do gateway (compartilhado no container)
                    cls._instance._ip = cls._instance._get_real_ip()     # Declarando IP do gateway
                    cls._instance._group = os.environ.get('Zato_GROUP', 'cloud/c1')
                    
                    logger.info(f"ID Manager Iniciado - ID: {cls._instance._id} - IP: {cls._instance._ip}")
       
        return cls._instance

    def _get_real_ip(self):

        ip = os.environ.get('Zato_GATEWAY_REAL_IP')    # Declaração do IP pela variável de ambiente
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

    @group.setter
    def group(self, value):
        self._group = value


class IdentityService(Service):
    """
    Serviço que expõe a identidade do Gateway (ID, IP e Grupo).
    Substitui IIDManagerService e IDLTGroupManager.
    """
    name = 'soft-iot.id.manager'

    def handle(self):
        # Acessa o Singleton de Identidade
        manager = IDManager()

        # Retorna os dados que seriam obtidos pelos métodos getID, getIP e getGroup
        self.response.payload = {
            "gateway_id": manager.id,
            "gateway_ip": manager.ip,
            "tangle_group": manager.group
        }