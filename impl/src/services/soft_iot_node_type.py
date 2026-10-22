# -*- coding: utf-8 -*-

import os
import threading
import logging
import random

from zato.server.service import Service

from soft_iot_id_manager import IDManager
from soft_iot_gateway_state import GatewayStateManager

logger = logging.getLogger('zato.node.type')

class NodeTypeManager:
    """
    Controlador Singleton para gerenciar o tipo e a conduta do nó.
    Substitui a lógica central do NodeType.java e as configurações do br.uefs.larsid.soft_iot.node_type.cfg.
    """

    _instance = None
    _lock = threading.Lock()  # Evita múltiplas instâncias com acessos simultâneos

    def __new__(cls):
        # Implementação do Singleton. Cria uma nova instância, se não houver.
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(NodeTypeManager, cls).__new__(cls)
                    
                    # Carrega as configurações das variáveis de ambiente
                    # Tipos: 1 - Honesto, 2 - Malicioso, 3 - Egoista, 4 - Perturbador
                    cls._instance._node_type = int(os.environ.get('Zato_NODE_TYPE', 1))

                    # Taxa de honestidade
                    cls._instance._honesty_rate = float(os.environ.get('Zato_HONESTY_RATE', 50.0))
                    
                    # Valor inicial da conduta
                    cls._instance._current_conduct = "HONEST"
                    
                    logger.info(f"NodeTypeManager iniciado - Tipo: {cls._instance._node_type} - Taxa de honestidade: {cls._instance._honesty_rate}%")
                    
                    # Inicializa a conduta baseada no tipo
                    cls._instance.define_conduct(False)

        return cls._instance

    def define_conduct(self, behavior_changed):
        """
        Define a conduta baseada no tipo e na reputação (para o Perturbador).
        Reflete a lógica de Disturbing.java e Malicious.java.
        """
        # 1 - Honesto 
        if self._node_type == 1:
            self._current_conduct = "HONEST"
            
        # 2 - Malicioso
        elif self._node_type == 2:
            # Mantém o sorteio probabilístico do Malicious.java
            random_number = random.uniform(0, 100)
            if random_number > self._honesty_rate:
                self._current_conduct = "MALICIOUS"
            else:
                self._current_conduct = "HONEST"
                
        # 3 - Egoísta
        elif self._node_type == 3:
            self._current_conduct = "SELFISH" 
            
        # 4 - Perturbador (Lógica fiel ao Java)
        elif self._node_type == 4:
            if behavior_changed:
                # Segunda Fase: Atingiu o limiar. Começa a perturbar baseado na taxa.
                random_number = random.uniform(0, 100)
                if random_number > self._honesty_rate:
                    self._current_conduct = "MALICIOUS"
                else:
                    self._current_conduct = "HONEST"
            else:
                # Primeira Fase: Finge ser 100% honesto para acumular reputação.
                self._current_conduct = "HONEST"


    @property
    def current_conduct(self):
        """Retorna a conduta atual."""
        return self._current_conduct

    @property
    def node_type(self):
        """Retorna o tipo base do nó."""
        return self._node_type

    @property
    def honesty_rate(self):
        """Retorna a taxa de honestidade configurada."""
        return self._honesty_rate
    

class NodeEvaluationService(Service):
    """
    Serviço que executa a conduta do nó ao avaliar um serviço.
    Substitui o método evaluateServiceProvider das classes de conduta Java.
    """
    name = 'soft-iot.node.evaluation'

    def handle(self):

        nt_manager = NodeTypeManager()
        # id_manager = IDManager()
        gs_manager = GatewayStateManager()

        # Dados da avaliação 
        data = self.request.payload
        provider_id = data.get('provider_id')
        behavior_changed = gs_manager.get_behavior_changed()
 
        service_evaluation = data.get('serviceEvaluation')
        # node_credibility = data.get('nodeCredibility')
        evaluation_value = data.get('value')

        # Consultando condulta
        nt_manager.define_conduct(behavior_changed)
        conduct = nt_manager.current_conduct

        if conduct == "SELFISH":
            # Egoísta não avalia 
            self.logger.info(f"Nó Egoísta ignorando avaliação para {provider_id}")
            self.response.payload = {"status": "ignored", "conduct_applied": conduct}
            return

        final_service_evaluation = int(service_evaluation) if service_evaluation is not None else 0
        final_evaluation_value = float(evaluation_value) if evaluation_value is not None else 0.0
        
        if conduct == "MALICIOUS":
            # Malicioso avalia como ruim
            self.logger.info(
                f"Ataque Malicioso: Alterando avaliação de {final_service_evaluation} para 0"
            )
            final_service_evaluation = 0
            final_evaluation_value = 0.0
            
        self.response.payload = {"status": "success", "conduct_applied": conduct, "final_evaluation_value": final_evaluation_value, "final_service_evaluation": final_service_evaluation}

        return
    

class ChangeDisturbingNodeBehaviorTask(Service):
    """
    Task para alterar o comportamento de um nó do tipo Perturbador (Tipo 4).
    Versão otimizada: Consulta a reputação local em vez de recalcular.
    """
    name = 'soft-iot.reputation.task.change_disturbing_behavior'

    REPUTATION_THRESHOLD = 0.9

    def handle(self):
        self.logger.info("Executando ChangeDisturbingNodeBehaviorTask...")

        nt_manager = NodeTypeManager()
        gs_manager = GatewayStateManager()

        if nt_manager.node_type != 4:
            self.response.payload = {"status": "ignored", "message": "not_disturbing_node"}
            return

        if  gs_manager.get_behavior_changed():
            self.response.payload = {"status": "ignored", "message": "already_changed"}
            return

        reputation_value = gs_manager.get_reputation()
        
        self.logger.info(f"Reputação lida do armazenamento local: {reputation_value}")

        if reputation_value > self.REPUTATION_THRESHOLD:
            self.logger.info(f"Limiar de {self.REPUTATION_THRESHOLD} atingido. Alterando a flag de comportamento.")
            
            gs_manager.set_behavior_changed(True)
            
            self.response.payload = {"status": "success", "behavior_changed": True}
        else:
            self.response.payload = {"status": "success", "behavior_changed": False}
        