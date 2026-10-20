# -*- coding: utf-8 -*-
"""
soft_iot_trust.py

Confiança: K-Means sobre credibilidades, reputação (média ponderada com prior)
e ajuste de credibilidade (confiabilidade + consistência).

Funções puras: recebem os itens já lidos da DLT e devolvem números.
Não importa Zato e não faz I/O, então pode ser testado fora do container.
"""

import logging
import math

import numpy as np
from sklearn.cluster import KMeans

from soft_iot_common import (
    MsgType, env_float, filter_by_type, sort_newest_first,
)

logger = logging.getLogger('zato.trust')

# K-Means
K_CLUSTERS = 4
KMEANS_MAX_ITER = 100
_PAD = -1.0                      # preenchimento quando há menos de K pontos

# Reputação
PRIOR = 0.5
REPUTATION_MIN, REPUTATION_MAX = -1.0, 1.0
NEUTRAL_REPUTATION = 0.5

# Credibilidade
DEFAULT_CREDIBILITY = 0.5
CREDIBILITY_MIN, CREDIBILITY_MAX = -1.0, 1.0
MIN_STEP = 0.01


# ---------------------------------------------------------------------------
# K-Means
# ---------------------------------------------------------------------------
def kmeans_trusted_credibilities(credibilities):
    """Valores (únicos) do cluster que contém a maior credibilidade."""
    data = list(credibilities)
    while len(data) < K_CLUSTERS:
        data.append(_PAD)

    X = np.array(data).reshape(-1, 1)
    model = KMeans(n_clusters=K_CLUSTERS, max_iter=KMEANS_MAX_ITER, n_init=10, random_state=42)
    labels = model.fit_predict(X)

    if logger.isEnabledFor(logging.DEBUG):
        cluster_map = {}
        for i, label in enumerate(labels):
            cluster_map.setdefault(label, []).append(data[i])
        for label, points in cluster_map.items():
            logger.debug(f"Cluster {label}: {points}")

    valid = [v for v in data if v != _PAD]
    max_value = max(valid) if valid else _PAD
    target_cluster = labels[data.index(max_value)]

    result = list({v for i, v in enumerate(data) if labels[i] == target_cluster and v != _PAD})
    logger.info(f"Pontos no cluster com maior valor: {result}")
    return result


# ---------------------------------------------------------------------------
# Reputação
# ---------------------------------------------------------------------------
def extract_evaluations(items):
    """Itens lidos da DLT -> [{source, value, credibility}] das REP_EVALUATION válidas."""
    evaluations = []
    for data in filter_by_type(items, MsgType.EVALUATION):
        try:
            evaluations.append({
                'source': data.get('source'),
                'value': float(data.get('value', 0.0)),
                'credibility': float(data.get('nodeCredibility', 0.0)),
            })
        except (ValueError, TypeError):
            continue
    return evaluations


def select_trusted_evaluations(evaluations):
    """Mantém as avaliações cujo avaliador está no cluster de maior credibilidade."""
    credibilities = [ev['credibility'] for ev in evaluations]
    logger.info(f"Executando K-Means sobre as credibilidades de {len(credibilities)} avaliadores")

    try:
        trusted = kmeans_trusted_credibilities(credibilities)
    except Exception as e:
        logger.error(f"Erro no K-Means: {e}")
        trusted = []

    if not trusted:
        logger.error("Falha no K-Means, utilizando todos os avaliadores como fallback.")
        trusted = credibilities

    selected = [
        ev for ev in evaluations
        if any(math.isclose(ev['credibility'], tc, rel_tol=1e-5) for tc in trusted)
    ]
    return selected or evaluations


def reputation_from_history(items, prior_weight=None):
    """
    Reputação de um nó a partir do histórico lido no índice dele.
    Retorna {"reputation", "evaluations_used", "status"} com status "success" ou "no_data".
    """
    if prior_weight is None:
        prior_weight = env_float('Zato_REP_PRIOR_WEIGHT', 5.0)

    evaluations = extract_evaluations(items)
    if not evaluations:
        return {"reputation": NEUTRAL_REPUTATION, "evaluations_used": 0, "status": "no_data"}

    trusted = select_trusted_evaluations(evaluations)

    total = sum(ev['value'] for ev in trusted) + (PRIOR * prior_weight)
    count = len(trusted) + prior_weight
    reputation = max(REPUTATION_MIN, min(REPUTATION_MAX, total / count))

    logger.info(
        f"[REPUTAÇÃO] Final: {reputation:.4f} | "
        f"Avaliações confiáveis: {len(trusted)} de {len(evaluations)}"
    )
    return {
        "reputation": round(reputation, 4),
        "evaluations_used": len(trusted),
        "status": "success",
    }


# ---------------------------------------------------------------------------
# Credibilidade
# ---------------------------------------------------------------------------
def latest_credibility(items, default=DEFAULT_CREDIBILITY):
    """Última credibilidade gravada (CRED_UPDATE) lida do índice CRED_<avaliador>."""
    updates = sort_newest_first(filter_by_type(items, MsgType.CRED_UPDATE))
    if not updates:
        return default
    return float(updates[0].get('credibility', default))


def last_evaluation_by(items, evaluator_id):
    """`serviceEvaluation` mais recente dado por `evaluator_id` no histórico do provedor (ou None)."""
    history = sort_newest_first(filter_by_type(items, MsgType.EVALUATION))
    for data in history:
        if data.get('source') == evaluator_id:
            return data.get('serviceEvaluation')
    return None


def compute_credibility(current_cred, evaluation_given, last_evaluation_given,
                        consensus_reputation, reliability_threshold=None,
                        consistency_threshold=None):
    """
    Confiabilidade = |consenso - nota dada|; consistência = |nota atual - nota anterior|.
    Na primeira avaliação (last_evaluation_given None) a consistência é máxima (0).
    """
    if reliability_threshold is None:
        reliability_threshold = env_float('Zato_RELIABILITY_THRESHOLD', 0.5)
    if consistency_threshold is None:
        consistency_threshold = env_float('Zato_CONSISTENCY_THRESHOLD', 0.5)

    if last_evaluation_given is None:
        last_evaluation_given = evaluation_given
    else:
        last_evaluation_given = float(last_evaluation_given)

    reliability = abs(consensus_reputation - evaluation_given)
    consistency = abs(evaluation_given - last_evaluation_given)

    is_reliable = reliability <= reliability_threshold
    is_consistent = consistency <= consistency_threshold

    if is_reliable and is_consistent:
        new_cred = current_cred + max(abs(current_cred) * 0.10, MIN_STEP)
    elif is_reliable:
        new_cred = current_cred + max(abs(current_cred) * 0.05, MIN_STEP)
    elif is_consistent:
        new_cred = current_cred - max(abs(current_cred) * 0.05, MIN_STEP)
    else:
        new_cred = current_cred - max(abs(current_cred) * 0.10, MIN_STEP)

    new_cred = max(CREDIBILITY_MIN, min(CREDIBILITY_MAX, new_cred))

    return {
        "old_credibility": round(current_cred, 4),
        "new_credibility": round(new_cred, 4),
        "reliability": round(reliability, 4),
        "consistency": round(consistency, 4),
    }