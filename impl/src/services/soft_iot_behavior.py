# -*- coding: utf-8 -*-
"""
soft_iot_behavior.py

Condutas simuladas (honesto, malicioso, egoísta, perturbador), isoladas da lógica real:
  - NodeTypeManager: define a conduta a cada consulta
  - apply_conduct_to_evaluation(): distorce/ignora a avaliação do provedor
  - build_offer(): lista de serviços oferecidos (falsifica se malicioso)
  - tarefa do perturbador (muda de conduta ao passar do limiar de reputação)

Serviço Zato: soft-iot.reputation.task.change_disturbing_behavior
"""

import logging
import random
import threading

from zato.server.service import Service

from soft_iot_common import (
    Conduct, NodeType, env_float, env_int, find_sensors_by_type,
)
from soft_iot_state import get_state

logger = logging.getLogger('zato.node.type')

NONEXISTENT_DEVICE = "nonexistentDevice"
NONEXISTENT_SENSOR = "nonexistentSensor"
DISTURBING_REPUTATION_THRESHOLD = 0.9


class NodeTypeManager:
    """Singleton com o tipo do nó (Zato_NODE_TYPE) e a taxa de honestidade (Zato_HONESTY_RATE)."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    inst = super(NodeTypeManager, cls).__new__(cls)
                    inst._node_type = env_int('Zato_NODE_TYPE', NodeType.HONEST)
                    inst._honesty_rate = env_float('Zato_HONESTY_RATE', 50.0)
                    inst._current_conduct = Conduct.HONEST
                    cls._instance = inst
                    logger.info(
                        f"NodeTypeManager iniciado - Tipo: {inst._node_type} - "
                        f"Taxa de honestidade: {inst._honesty_rate}%"
                    )
                    inst.define_conduct(False)
        return cls._instance

    def _draw(self):
        """Sorteio probabilístico: acima da taxa de honestidade, age de forma maliciosa."""
        return Conduct.MALICIOUS if random.uniform(0, 100) > self._honesty_rate else Conduct.HONEST

    def define_conduct(self, behavior_changed):
        """Define (e retorna) a conduta para a próxima ação."""
        if self._node_type == NodeType.HONEST:
            conduct = Conduct.HONEST
        elif self._node_type == NodeType.MALICIOUS:
            conduct = self._draw()
        elif self._node_type == NodeType.SELFISH:
            conduct = Conduct.SELFISH
        elif self._node_type == NodeType.DISTURBING:
            # Fase 1: honesto (acumula reputação). Fase 2: age conforme a taxa.
            conduct = self._draw() if behavior_changed else Conduct.HONEST
        else:
            conduct = self._current_conduct

        self._current_conduct = conduct
        return conduct

    @property
    def current_conduct(self):
        return self._current_conduct

    @property
    def node_type(self):
        return self._node_type


def current_conduct():
    """Conduta para a ação atual, já considerando o estado do perturbador."""
    return NodeTypeManager().define_conduct(get_state().get_behavior_changed())


def apply_conduct_to_evaluation(provider_id, service_evaluation, value):
    """
    Aplica a conduta à nota do provedor.
    Retorna {"conduct_applied", "final_service_evaluation", "final_evaluation_value"}.
    Egoísta mantém as notas (a decisão de não publicar é do chamador, via conduct_applied).
    """
    conduct = current_conduct()

    final_service_evaluation = int(service_evaluation) if service_evaluation is not None else 0
    final_evaluation_value = float(value) if value is not None else 0.0

    if conduct == Conduct.SELFISH:
        logger.info(f"Nó Egoísta ignorando avaliação para {provider_id}")
    elif conduct == Conduct.MALICIOUS:
        logger.info(f"Ataque Malicioso: Alterando avaliação de {final_service_evaluation} para 0")
        final_service_evaluation = 0
        final_evaluation_value = 0.0

    return {
        "conduct_applied": conduct,
        "final_service_evaluation": final_service_evaluation,
        "final_evaluation_value": final_evaluation_value,
    }


def build_offer(requested_service):
    """Serviços [{device_id, sensor_id}] a oferecer para o serviço pedido."""
    if current_conduct() == Conduct.MALICIOUS:
        logger.warning(
            f"Conduta MALICIOSA ativa: ocultando sensores reais e publicando falsos para '{requested_service}'."
        )
        return [{"device_id": NONEXISTENT_DEVICE, "sensor_id": NONEXISTENT_SENSOR}]

    return find_sensors_by_type(requested_service)


class ChangeDisturbingNodeBehaviorTask(Service):
    """Nó perturbador (tipo 4): ativa a fase de perturbação quando a reputação local passa do limiar."""
    name = 'soft-iot.reputation.task.change_disturbing_behavior'

    def handle(self):
        if NodeTypeManager().node_type != NodeType.DISTURBING:
            self.response.payload = {"status": "ignored", "message": "not_disturbing_node"}
            return

        state = get_state()
        if state.get_behavior_changed():
            self.response.payload = {"status": "ignored", "message": "already_changed"}
            return

        reputation = state.get_reputation()
        self.logger.info(f"Reputação lida do armazenamento local: {reputation}")

        if reputation > DISTURBING_REPUTATION_THRESHOLD:
            self.logger.info(
                f"Limiar de {DISTURBING_REPUTATION_THRESHOLD} atingido. Alterando a flag de comportamento."
            )
            state.set_behavior_changed(True)
            self.response.payload = {"status": "success", "behavior_changed": True}
        else:
            self.response.payload = {"status": "success", "behavior_changed": False}