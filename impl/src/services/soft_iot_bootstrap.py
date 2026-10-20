# -*- coding: utf-8 -*-
"""
soft_iot_bootstrap.py

Inicialização em ordem, num único job one_time:
  1. identidade (UUID/IP/grupo)  2. banco de estado  3. storage local (MQTT + SQLite)  4. ZMQ

Serviço Zato: soft-iot.bootstrap
"""

from zato.server.service import Service

from soft_iot_behavior import NodeTypeManager
from soft_iot_common import IDManager
from soft_iot_dlt import start_zmq
from soft_iot_state import get_state
from soft_iot_storage import LocalStorageController


class BootstrapService(Service):
    name = 'soft-iot.bootstrap'

    def _step(self, label, fn):
        try:
            fn()
            self.logger.info(f"[BOOTSTRAP] {label}: ok")
            return True
        except Exception as e:
            self.logger.error(f"[BOOTSTRAP] {label}: falhou ({e})")
            return False

    def _start_storage(self):
        controller = LocalStorageController()
        if controller.is_running:
            self.logger.info("Controlador Soft-IoT Storage já está rodando.")
        else:
            controller.start(self.logger)

    def handle(self):
        results = {
            "identity": self._step("Identidade", IDManager),
            "state": self._step("Estado local", get_state),
            "node_type": self._step("Tipo de nó", NodeTypeManager),
            "storage": self._step("Storage local", self._start_storage),
            "zmq": self._step("ZMQ", lambda: start_zmq(self.server)),
        }
        self.response.payload = {"status": "success" if all(results.values()) else "partial", "steps": results}