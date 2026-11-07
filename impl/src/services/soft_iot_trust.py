# -*- coding: utf-8 -*-

import os
import math
import json
import logging
import numpy as np
from sklearn.cluster import KMeans
import time
from zato.server.service import Service


logger = logging.getLogger('zato.trust')

# Modos de uso da credibilidade no cálculo da reputação (Zato_REP_CREDIBILITY_MODE)
#   0 -> sem credibilidade: média das notas (serviceEvaluation), sem K-Means
#   1 -> credibilidade no momento da avaliação: usa 'value' e 'nodeCredibility' gravados
#        na avaliação; K-Means sobre a credibilidade de cada avaliação
#   2 -> credibilidade atual: serviceEvaluation x credibilidade mais recente do avaliador;
#        K-Means sobre a credibilidade atual de cada avaliador
CREDIBILITY_MODES = {
    0: 'sem credibilidade',
    1: 'credibilidade no momento da avaliação',
    2: 'credibilidade atual',
}
DEFAULT_CREDIBILITY_MODE = 2


def get_credibility_mode(log):
    """Lê Zato_REP_CREDIBILITY_MODE. Valor ausente ou inválido usa o padrão."""
    raw = os.environ.get('Zato_REP_CREDIBILITY_MODE', str(DEFAULT_CREDIBILITY_MODE))
    try:
        mode = int(str(raw).strip())
    except ValueError:
        mode = None

    if mode not in CREDIBILITY_MODES:
        log.warning(
            f"Zato_REP_CREDIBILITY_MODE inválido ({raw!r}). "
            f"Usando {DEFAULT_CREDIBILITY_MODE} ({CREDIBILITY_MODES[DEFAULT_CREDIBILITY_MODE]})."
        )
        mode = DEFAULT_CREDIBILITY_MODE
    return mode


def get_latest_credibility(service, evaluator_id):
    """
    Credibilidade mais recente do avaliador, lida de GW_CRED_<evaluator_id>.
    Retorna None se ele ainda não tem credibilidade publicada.
    """
    tangle_res = service.invoke('soft-iot.dlt.client.api.read_index', {'index': f"GW_CRED_{evaluator_id}"})

    if not (isinstance(tangle_res, list) and tangle_res):
        return None

    valid_creds = []
    for tx in tangle_res:
        data_block = tx.get('data')

        if isinstance(data_block, str):
            try:
                data_block = json.loads(data_block)
            except json.JSONDecodeError:
                continue

        if isinstance(data_block, dict) and data_block.get('type') == 'GW_CREDIBILITY':
            valid_creds.append(data_block)

    if not valid_creds:
        return None

    # Ordena as credibilidades pelo timestamp (mais recente primeiro)
    valid_creds.sort(key=lambda x: int(x.get('timestamp', 0)), reverse=True)
    return valid_creds[0].get('credibility', 0.5)


class ReputationOrchestrator(Service):
    """
    Serviço principal que coordena o cálculo de reputação de um nó.
    O uso da credibilidade depende de Zato_REP_CREDIBILITY_MODE (ver CREDIBILITY_MODES).
    """
    name = 'soft-iot.reputation.orchestrator'

    def handle(self):
        target_node_id = self.request.payload.get('node_id')

        if not target_node_id:
            self.response.payload = {"status": "error", "message": "node_id é obrigatório"}
            return

        mode = get_credibility_mode(self.logger)

        # Busca avaliações históricas na Tangle para este nó
        self.logger.info(f"Buscando avaliações na Tangle para o nó: {target_node_id}")
        tangle_res = self.invoke('soft-iot.dlt.client.api.read_index', {'index': f"GW_EVAL_{target_node_id}"})

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

                    # Adiciona transações de avaliação (GW_EVALUATION)
                    if content.get('type') == 'GW_EVALUATION':
                        try:
                            service_evaluation = float(content.get('serviceEvaluation'))
                        except (ValueError, TypeError):
                            service_evaluation = None

                        valid_evaluations.append({
                            'source': content.get('source'),
                            'value': float(content.get('value', 0.0)),
                            'credibility': float(content.get('nodeCredibility', 0.0)),
                            'service_evaluation': service_evaluation
                        })
                except (ValueError, TypeError, AttributeError):
                    continue

        # Os modos 0 e 2 usam a nota pura (e o modo 2 também o avaliador)
        if mode == 0:
            valid_evaluations = [ev for ev in valid_evaluations if ev['service_evaluation'] is not None]
        elif mode == 2:
            valid_evaluations = [ev for ev in valid_evaluations
                                 if ev['service_evaluation'] is not None and ev['source']]

        # Se não há dados, retorna a reputação inicial (0.5)
        if len(valid_evaluations) == 0:
            self.logger.warning(f"Sem dados na Tangle para {target_node_id}. Retornando 0.5 (neutro).")
            self.response.payload = {"node_id": target_node_id, "reputation": 0.5, "status": "no_data",
                                     "credibility_mode": mode}
            return

        if mode == 0:
            scores = self._scores_without_credibility(valid_evaluations)
        elif mode == 1:
            scores = self._scores_evaluation_time_credibility(valid_evaluations)
        else:
            scores = self._scores_current_credibility(valid_evaluations)

        # Calcula a Reputação Final: Média aritmética das avaliações dos nós presentes no cluster confiável

        PRIOR = 0.5
        PRIOR_WEIGHT = float(os.environ.get('Zato_REP_PRIOR_WEIGHT', '5'))

        sum_values = sum(scores) + (PRIOR * PRIOR_WEIGHT)
        total_evaluations  = len(scores) + PRIOR_WEIGHT

        final_reputation = sum_values / total_evaluations
        final_reputation = max(0.0, min(1.0, final_reputation))

        self.logger.info(
            f"[RESULTADO ORCHESTRATOR] Nó: {target_node_id} | "
            f"Modo: {mode} ({CREDIBILITY_MODES[mode]}) | "
            f"Reputação Final: {final_reputation:.4f} | "
            f"Avaliações Confiáveis Utilizadas: {len(scores)} de {len(valid_evaluations)}"
        )

        self.response.payload = {
            "node_id": target_node_id,
            "reputation": round(final_reputation, 4),
            "evaluations_used": len(scores),
            "credibility_mode": mode,
            "status": "success"
        }

    def _scores_without_credibility(self, valid_evaluations):
        """Modo 0: todas as avaliações entram, com a nota pura (0 ou 1)."""
        return [ev['service_evaluation'] for ev in valid_evaluations]

    def _scores_evaluation_time_credibility(self, valid_evaluations):
        """Modo 1: credibilidade gravada no momento de cada avaliação."""

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

        return [ev['value'] for ev in trusted_evaluations]

    def _scores_current_credibility(self, valid_evaluations):
        """
        Modo 2: credibilidade atual de cada avaliador.
        O K-Means agrupa os avaliadores (um valor por avaliador) e entram só as
        avaliações dos mais credíveis, cada uma valendo serviceEvaluation x credibilidade atual.
        """
        current_creds = {}
        for source in sorted({ev['source'] for ev in valid_evaluations}):
            cred = get_latest_credibility(self, source)
            current_creds[source] = float(cred) if cred is not None else 0.5

        self.logger.info(f"Credibilidades atuais dos avaliadores: {current_creds}")

        credibilities = list(current_creds.values())
        self.logger.info(f"Executando K-Means sobre as credibilidades atuais de {len(credibilities)} avaliadores")
        kmeans_res = self.invoke('soft-iot.kmeans.credibility', {'data': credibilities})

        trusted_credibilities = kmeans_res if isinstance(kmeans_res, list) else []

        if not trusted_credibilities:
            self.logger.error("Falha no K-Means, utilizando todos os avaliadores como fallback.")
            trusted_credibilities = credibilities

        # Avaliadores cuja credibilidade atual está no cluster de maior credibilidade
        trusted_sources = {
            source for source, cred in current_creds.items()
            if any(math.isclose(cred, tc, rel_tol=1e-5) for tc in trusted_credibilities)
        }

        if not trusted_sources:
            trusted_sources = set(current_creds)  # Fallback de segurança

        self.logger.info(f"Avaliadores mais credíveis: {sorted(trusted_sources)}")

        return [ev['service_evaluation'] * current_creds[ev['source']]
                for ev in valid_evaluations if ev['source'] in trusted_sources]


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
        latest_cred = get_latest_credibility(self, evaluator_id)
        if latest_cred is not None:
            current_cred = latest_cred
            self.logger.info(f"Credibilidade recuperada para {evaluator_id}: {current_cred}")

        # Coleta da última avaliação
        last_evaluation_given = None

        if provider_id:
            # Busca todo o histórico de avaliações que o provedor já recebeu
            provider_history_raw = self.invoke('soft-iot.dlt.client.api.read_index', {'index': f"GW_EVAL_{provider_id}"})
            
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
                    
                    if isinstance(tx_data, dict) and tx_data.get('type') == 'GW_EVALUATION':
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

        # Credibilidade limitada ao intervalo [0, 1]
        if new_cred > 1.0:
            new_cred = 1.0
        elif new_cred < 0.0:
            new_cred = 0.0


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