# -*- coding: utf-8 -*-

import socket
import uuid
import os
import threading
import logging

from zato.server.service import Service


logger = logging.getLogger('zato.id.manager')

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
                    cls._instance._id = str(uuid.uuid4())                # Gerando ID do gateway
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