# -*- coding: utf-8 -*-

import os
import math
import json
import logging
import numpy as np
from sklearn.cluster import KMeans
import time
from zato.server.service import Service


logger = logging.getLogger('zato.reputation')

class ReputationOrchestrator(Service):
    """
    Serviço principal que coordena o cálculo de reputação de um nó.
    """
    name = 'soft-iot.reputation.orchestrator'

    def handle(self):
        target_node_id = self.request.payload.get('node_id')
        
        if not target_node_id:
            self.response.payload = {"status": "error", "message": "node_id é obrigatório"}
            return

        # Busca avaliações históricas na Tangle para este nó
        self.logger.info(f"Buscando avaliações na Tangle para o nó: {target_node_id}")
        tangle_res = self.invoke('soft-iot.dlt.client.api.read_index', {'index': target_node_id})
        
        valid_evaluations = []
        
        # Extração segura dos dados, considerando possíveis strings escapadas
        if isinstance(tangle_res, list):
            for tx in tangle_res:
                try:
                    content = tx.get('data', {})
                    if isinstance(content, str):
                        try:
                            content = json.loads(content)
                        except json.JSONDecodeError:
                            continue
                    
                    # Adiciona transações de avaliação (REP_EVALUATION)
                    if content.get('type') == 'REP_EVALUATION':
                        valid_evaluations.append({
                            'source': content.get('source'),
                            'value': float(content.get('value', 0.0)),
                            'credibility': float(content.get('nodeCredibility', 0.0))
                        })
                except (ValueError, TypeError, AttributeError):
                    continue

        # Se não há dados, retorna a reputação inicial (0.5)
        if len(valid_evaluations) == 0:
            self.logger.warning(f"Sem dados na Tangle para {target_node_id}. Retornando 0.5 (neutro).")
            self.response.payload = {"node_id": target_node_id, "reputation": 0.5, "status": "no_data"}
            return

        # Executa o K-Means sobre as credibilidades
        credibilities = [eval_data['credibility'] for eval_data in valid_evaluations]
        
        self.logger.info(f"Executando K-Means sobre as credibilidades de {len(credibilities)} avaliadores")
        kmeans_res = self.invoke('soft-iot.kmeans.credibility', {'data': credibilities})
        
        # Maiores credibilidades
        trusted_credibilities = kmeans_res if isinstance(kmeans_res, list) else []

        if not trusted_credibilities:
            self.logger.error("Falha no K-Means, utilizando todos os avaliadores como fallback.")
            trusted_credibilities = credibilities

        # Filtra as avaliações: mantem apenas aquelas cuja credibilidade está no cluster seleto
        trusted_evaluations = []
        for ev in valid_evaluations:
            for tc in trusted_credibilities:
                # Uso de math.isclose para evitar problemas de precisão com floats
                if math.isclose(ev['credibility'], tc, rel_tol=1e-5):
                    trusted_evaluations.append(ev)
                    break

        if not trusted_evaluations:
            trusted_evaluations = valid_evaluations  # Fallback de segurança

        # Calcula a Reputação Final: Média aritmética das avaliações dos nós presentes no cluster confiável

        PRIOR = 0.5
        PRIOR_WEIGHT = float(os.environ.get('Zato_REP_PRIOR_WEIGHT', '5'))

        sum_values = sum(ev['value'] for ev in trusted_evaluations) + (PRIOR * PRIOR_WEIGHT)
        total_evaluations  = len(trusted_evaluations) + PRIOR_WEIGHT

        final_reputation = sum_values / total_evaluations
        final_reputation = max(-1.0, min(1.0, final_reputation))

        self.logger.info(
            f"[RESULTADO ORCHESTRATOR] Nó: {target_node_id} | "
            f"Reputação Final: {final_reputation:.4f} | "
            f"Avaliações Confiáveis Utilizadas: {len(trusted_evaluations)} de {len(valid_evaluations)}"
        )
        
        self.response.payload = {
            "node_id": target_node_id,
            "reputation": round(final_reputation, 4),
            "evaluations_used": len(trusted_evaluations),
            "status": "success"
        }


class CredibilityManager(Service):
    """
    Gerencia a credibilidade de quem avalia.
    Implementa as lógicas de Reliability (Confiabilidade) e Consistency (Consistência).
    """
    name = 'soft-iot.reputation.credibility.manager'

    def handle(self):

        # Coleta dos dados de entrada do payload
        payload = self.request.payload
        evaluator_id = payload.get('evaluator_id') # ID do nó avaliador
        provider_id = payload.get('provider_id') # ID do nó provedor
        evaluation_given = float(payload.get('evaluation_given')) # Nota atual
        consensus_reputation = float(payload.get('consensus_reputation'))

        if not evaluator_id:
            self.response.payload = {"status": "error", "message": "Evaluator ID missing"}
            return

        # Valor default
        current_cred = 0.5

        # Coleta da última credibilidade
        tangle_res = self.invoke('soft-iot.dlt.client.api.read_index', {'index': f"CRED_{evaluator_id}"})
            
        # Validação de Defesa: Verifica se retornou dados válidos
        if isinstance(tangle_res, list) and tangle_res:
            valid_creds = []
            
            for tx in tangle_res:
                data_block = tx.get('data')
                
                if isinstance(data_block, str):
                    try:
                        data_block = json.loads(data_block)
                    except json.JSONDecodeError:
                        continue
                
                if isinstance(data_block, dict) and data_block.get('type') == 'CRED_UPDATE':
                    valid_creds.append(data_block)
            
            # Ordena as credibilidades pelo timestamp 
            valid_creds.sort(key=lambda x: int(x.get('timestamp', 0)), reverse=True)
            
            if valid_creds:
                current_cred = valid_creds[0].get('credibility', 0.5)
                self.logger.info(f"Credibilidade recuperada para {evaluator_id}: {current_cred}")

        # Coleta da última avaliação
        last_evaluation_given = None

        if provider_id:
            # Busca todo o histórico de avaliações que o provedor já recebeu
            provider_history_raw = self.invoke('soft-iot.dlt.client.api.read_index', {'index': provider_id})
            
            valid_history = []

            # Primeiro filtramos e extraímos os dados válidos
            if isinstance(provider_history_raw, list):
                for tx in provider_history_raw:
                    tx_data = tx.get('data')
                    
                    if isinstance(tx_data, str):
                        try:
                            tx_data = json.loads(tx_data)
                        except json.JSONDecodeError:
                            tx_data = {}
                    
                    if isinstance(tx_data, dict) and tx_data.get('type') == 'REP_EVALUATION':
                        valid_history.append(tx_data)

            # Ordena o histórico do mais recente para o mais antigo pelo Timestamp
            valid_history.sort(key=lambda x: int(x.get('timestamp', 0)), reverse=True)

            # Percorre a lista ordenada. A primeira ocorrência será garantidamente a mais recente.
            for tx_data in valid_history:
                if tx_data.get('source') == evaluator_id:
                    last_evaluation_given = tx_data.get('serviceEvaluation')
                    break

        # Tratamento da primeira avaliação
        if last_evaluation_given is None:
            self.logger.info(f"Primeira vez que {evaluator_id} avalia {provider_id}. Consistência considerada máxima.")
            last_evaluation_given = evaluation_given
        else:
            last_evaluation_given = float(last_evaluation_given)
        

        # Confiabilidade: Comparação entre a nota dada e o consenso da rede
        reliability = abs(consensus_reputation - evaluation_given)
        
        # Consistência: Comparação entre a nota atual e a conduta anterior do mesmo nó
        consistency = abs(evaluation_given - last_evaluation_given)

        # Limiares de decisão via variáveis de ambiente 
        RELIABILITY_THRESHOLD = float(os.environ.get('Zato_RELIABILITY_THRESHOLD', '0.5'))
        CONSISTENCY_THRESHOLD = float(os.environ.get('Zato_CONSISTENCY_THRESHOLD', '0.5'))

        MIN_STEP = 0.01

        new_cred = current_cred

        is_reliable = reliability <= RELIABILITY_THRESHOLD
        is_consistent = consistency <= CONSISTENCY_THRESHOLD

        # Cenário ideal
        if is_reliable and is_consistent:
            ajuste = max(abs(current_cred) * 0.10, MIN_STEP)
            new_cred = current_cred + ajuste

        # Apenas consenso com a avaliação da rede
        elif is_reliable:
            ajuste = max(abs(current_cred) * 0.05, MIN_STEP)
            new_cred = current_cred + ajuste

        # Apenas consenso com a avaliação anterior do próprio nó
        elif is_consistent:
            ajuste = max(abs(current_cred) * 0.05, MIN_STEP)
            new_cred = current_cred - ajuste

        # As duas métricas são maiores que o limite
        else:
            ajuste = max(abs(current_cred) * 0.10, MIN_STEP)
            new_cred = current_cred - ajuste

        if new_cred > 1.0:
            new_cred = 1.0
        elif new_cred < -1.0:
            new_cred = -1.0


        # Resposta final do serviço
        self.response.payload = {
            "old_credibility": round(current_cred, 4),
            "new_credibility": round(new_cred, 4),
            "reliability": round(reliability, 4),
            "consistency": round(consistency, 4)
        }


class KMeansCredibilityService(Service):
    """
    Serviço que executa o algoritmo K-Means sobre uma lista de dados,
    retornando os pontos do cluster que contém o maior valor.
    Equivalente ao KMeansServiceCredibility do Java.
    """
    name = 'soft-iot.kmeans.credibility'

    def handle(self):
        start_time = time.time()

        # Recebe os dados do payload da requisição JSON
        # Esperado: {"data": [10.5, 20.1, 5.0, ...]}
        raw_data = self.request.payload.get('data', [])
        
        if not raw_data:
            self.response.payload = {"error": "A lista 'data' não foi fornecida."}
            self.response.status_code = 400
            return

        k = 4
        max_iterations = 100

        # Copia a lista para evitar mutações indesejadas
        data = list(raw_data)

        # Preenche com -1.0f 
        while len(data) < k:
            data.append(-1.0)

        # Prepara os dados e executa o K-Means
        # O scikit-learn exige uma matriz 2D para as features (n_samples, n_features)
        X = np.array(data).reshape(-1, 1)
        
        # Instancia e treina o modelo
        kmeans = KMeans(n_clusters=k, max_iter=max_iterations, n_init=10, random_state=42)
        clusters_of_points = kmeans.fit_predict(X)

        # Organiza pontos por cluster para os logs 
        cluster_map = {}
        for i, cluster_id in enumerate(clusters_of_points):
            cluster_map.setdefault(cluster_id, []).append(data[i])

        self.logger.info("Clusters designados:")
        for c_id, points in cluster_map.items():
            self.logger.info(f"Cluster {c_id}: {points}")

        # Encontra o cluster que contém o maior valor original
        # Achamos o maior valor ignorando os -1.0 que injetamos
        valid_data = [val for val in data if val != -1.0]
        max_value = max(valid_data) if valid_data else -1.0
        
        # Pega o índice desse maior valor na lista original para descobrir seu cluster
        max_index = data.index(max_value)
        target_cluster = clusters_of_points[max_index]

        # Filtra os valores únicos desse cluster 
        # O uso de set() garante que não haverá valores repetidos
        result_set = set()
        for i, val in enumerate(data):
            if clusters_of_points[i] == target_cluster and val != -1.0:
                result_set.add(val)

        points_in_target_cluster = list(result_set)
        
        self.logger.info(f"Pontos no cluster com maior valor: {points_in_target_cluster}")

        duration = time.time() - start_time
        self.logger.info(f"Tempo de execução: {duration:.6f} segundos")

        self.response.payload = points_in_target_cluster