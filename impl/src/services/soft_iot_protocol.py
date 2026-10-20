# -*- coding: utf-8 -*-
"""
soft_iot_protocol.py

Protocolo de interação entre gateways:
  Descoberta : check_nodes_services -> (ZMQ: tratadores REQ/RES) -> wait_nodes_responses
               -> select_best_node
  Consumo    : request_node_service (HTTP direto, avaliação, credibilidade, publicação)
  Reputação  : calculate (reputação própria)

Máquina de estados da requisição (RequestStatus):
  WAITING_RESPONSES -> CHOOSING_BETTER_NODE -> REQUESTING_SERVICE -> FINISHED
  (ou FAILED / FAILED_NO_SERVICES / FAILED_DATA_CONSUMPTION / TIMEOUT)
"""

import logging
import random
import time
from datetime import datetime, timezone

import requests

from zato.server.service import Service

import soft_iot_trust as trust
from soft_iot_behavior import apply_conduct_to_evaluation, build_offer
from soft_iot_common import (
    AVAILABLE_SERVICES, GATEWAY_HTTP_PORT, INDEX_DISCOVERY, Conduct, IDManager,
    RequestStatus, ServiceName, build_credibility_update, build_evaluation,
    build_service_request, build_service_response, credibility_index,
)
from soft_iot_dlt import read_index, write_message
from soft_iot_state import get_state

logger = logging.getLogger('zato.protocol')

REQUEST_TIMEOUT_SECONDS = 120     # requisição em andamento além disso vira TIMEOUT
DEFAULT_WAIT_SECONDS = 10         # janela de coleta de propostas
HTTP_TIMEOUT = 10


# ===========================================================================
# Helpers
# ===========================================================================
def _reputation_of(node_id):
    """Reputação de um nó a partir do histórico dele na Tangle."""
    return trust.reputation_from_history(read_index(node_id))


def _update_credibility(evaluator_id, provider_id, evaluation_given, consensus_reputation):
    """Lê o histórico na DLT e calcula a nova credibilidade do avaliador."""
    current_cred = trust.latest_credibility(read_index(credibility_index(evaluator_id)))
    logger.info(f"Credibilidade atual de {evaluator_id}: {current_cred}")

    last_evaluation = None
    if provider_id:
        last_evaluation = trust.last_evaluation_by(read_index(provider_id), evaluator_id)
        if last_evaluation is None:
            logger.info(f"Primeira vez que {evaluator_id} avalia {provider_id}. Consistência considerada máxima.")

    return trust.compute_credibility(
        current_cred=current_cred,
        evaluation_given=float(evaluation_given),
        last_evaluation_given=last_evaluation,
        consensus_reputation=float(consensus_reputation),
    )


def _consume_services(ip_target, services):
    """GET direto em cada (device, sensor) oferecido. Retorna (dados_coletados, houve_erro)."""
    collected, has_error = [], False

    for service in services:
        device_id = service.get('device_id')
        sensor_id = service.get('sensor_id')
        url = f"http://{ip_target}:{GATEWAY_HTTP_PORT}/soft-iot/devices/{device_id}/sensors/{sensor_id}/data/latest"

        try:
            logger.info(f"Requisitando dados: {url}")
            response = requests.get(url, timeout=HTTP_TIMEOUT)

            if response.status_code == 200:
                sensor_item = response.json().get('data')
                logger.info(f"Dado recebido com sucesso de {device_id}/{sensor_id}: {sensor_item}")
                collected.append({"device": device_id, "sensor": sensor_id, "data": sensor_item})
            else:
                error_msg = response.json().get('error', 'Erro desconhecido')
                logger.warning(
                    f"Falha ao obter dado de {device_id}/{sensor_id}. "
                    f"Status: {response.status_code}. Erro do nó: {error_msg}"
                )
                has_error = True

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de rede ao tentar acessar o IP {ip_target}: {e}")
            has_error = True

    return collected, has_error


# ===========================================================================
# Descoberta
# ===========================================================================
class CheckNodesServicesTask(Service):
    """Sorteia um serviço e publica REP_SVC_REQ em REP_HAS_SVC (broadcast)."""
    name = 'soft-iot.reputation.task.check_nodes_services'

    def handle(self):
        state = get_state()
        last_id, last_status = state.get_last_request_status()

        # Bloqueio de concorrência: já existe pedido em andamento?
        if last_status in RequestStatus.IN_PROGRESS:
            start_time_str = state.get_start_request_time(last_id)
            if start_time_str:
                started = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                elapsed = (datetime.now(timezone.utc) - started).total_seconds()

                if elapsed > REQUEST_TIMEOUT_SECONDS:
                    self.logger.warning(f"TIMEOUT: A requisição {last_id} excedeu o limite com status {last_status}!")
                    state.finalize_request(last_id, RequestStatus.TIMEOUT)
                else:
                    self.logger.info(f"Requisição {last_id} em andamento.")
                    self.response.payload = {"status": "Already requesting"}
                    return

        chosen_service = random.choice(AVAILABLE_SERVICES)
        self.logger.info(f"Serviço escolhido aleatoriamente para requisição: {chosen_service}")

        identity = IDManager()
        request_id = state.create_request(identity.id)
        request_data = build_service_request(identity.id, identity.group, chosen_service, request_id)

        try:
            res = write_message(INDEX_DISCOVERY, request_data)

            if res.get("status") == "success":
                self.logger.info(f"Requisição do serviço {chosen_service} publicada no índice '{INDEX_DISCOVERY}'.")
                self.invoke_async(ServiceName.TASK_WAIT_RESPONSES, {'request_id': request_id}, None)
                self.response.payload = {"status": "Request created"}
            else:
                self.logger.warning(f"Falha na rede ao publicar requisição: {res}")
                state.finalize_request(request_id, RequestStatus.FAILED)
                self.response.payload = {"status": "Tangle write failed", "tangle_response": res}

        except Exception as e:
            self.logger.error(f"Erro crítico ao solicitar serviço: {e}")
            state.finalize_request(request_id, RequestStatus.FAILED)
            self.response.payload = {"status": "Internal Error"}


class ZMQRequestHandlerService(Service):
    """REP_SVC_REQ recebido pela ponte: responde com a oferta (real ou falsa, conforme a conduta)."""
    name = ServiceName.ZMQ_HANDLER_REQUEST

    def handle(self):
        payload = self.request.payload
        requested_service = payload.get('requestedService')
        requester_id = payload.get('source')
        id_request = payload.get('idRequest')

        if not requested_service or not requester_id:
            logger.warning("Requisição recebida com parâmetros nulos.")
            return

        identity = IDManager()
        if requester_id == identity.id:
            return  # a própria mensagem voltando pela ponte

        logger.info(f"Processando requisição de serviço '{requested_service}' do nó {requester_id}.")

        matched_services = build_offer(requested_service)
        if not matched_services:
            logger.info(f"O nó local não possui o serviço '{requested_service}'. Ignorando.")
            return

        logger.info(f"Encontrados {len(matched_services)} sensores compatíveis. Preparando REP_SVC_RES.")
        response_data = build_service_response(
            target=requester_id,
            id_request=id_request,
            source=identity.id,
            ip_source=identity.ip,
            group=identity.group,
            services=matched_services,
        )

        try:
            self.invoke_async('soft-iot.dlt.client.api.write',
                              {"index": INDEX_DISCOVERY, "data": response_data})
            logger.info(f"Oferta de serviço encaminhada para gravação na Tangle (Nó {requester_id}).")
        except Exception as e:
            logger.error(f"Erro interno ao acionar a API local de DLT: {e}")


class ZMQResponseHandlerService(Service):
    """REP_SVC_RES recebido pela ponte: guarda a proposta se for para este gateway e ainda estiver aberta."""
    name = ServiceName.ZMQ_HANDLER_RESPONSE

    def handle(self):
        payload = self.request.payload

        if payload.get('target') != IDManager().id:
            return

        id_request = payload.get('idRequest')
        source = payload.get('source')
        ip_source = payload.get('ipSource')

        state = get_state()
        if state.get_request_status(id_request) == RequestStatus.WAITING_RESPONSES:
            logger.info(f"Recebida proposta válida do provedor {source} (IP: {ip_source}).")
            state.save_response(
                id_request, source, ip_source, payload.get('target'),
                payload.get('services'), payload.get('group'),
            )


class WaitNodesResponsesTask(Service):
    """Mantém a janela de coleta aberta e, ao fim, avança para a escolha do melhor nó."""
    name = ServiceName.TASK_WAIT_RESPONSES

    def handle(self):
        payload = self.request.payload or {}
        request_id = payload.get('request_id')
        wait_time = payload.get('wait_time', DEFAULT_WAIT_SECONDS)

        if not request_id:
            self.logger.error("Falha Crítica: Nenhum ID de requisição fornecido para a tarefa de espera.")
            self.response.payload = {"status": "error", "message": "Missing ID"}
            return

        self.logger.info(f"Iniciando janela de coleta de {wait_time}s para a requisição {request_id}...")
        time.sleep(wait_time)

        state = get_state()
        current_status = state.get_request_status(request_id)

        if current_status == RequestStatus.WAITING_RESPONSES:
            self.logger.info(f"Janela concluída. Atualizando status da requisição {request_id} para iniciar cálculo.")
            state.update_request_status(request_id, RequestStatus.CHOOSING_BETTER_NODE)
            self.invoke_async(ServiceName.TASK_SELECT_BEST_NODE, {"request_id": request_id}, None)
        else:
            self.logger.warning(
                f"A requisição {request_id} mudou de estado prematuramente "
                f"(Status atual: {current_status}). Abortando transição."
            )


class SelectBestNodeTask(Service):
    """Ranqueia os provedores que responderam pela reputação e escolhe o maior score."""
    name = ServiceName.TASK_SELECT_BEST_NODE

    def handle(self):
        request_id = self.request.payload.get('request_id')
        if not request_id:
            self.logger.error("request_id não fornecido para SelectBestNodeTask.")
            return

        state = get_state()
        responses = state.get_responses_for_request(request_id)

        if not responses:
            self.logger.warning(f"Nenhuma resposta recebida para a requisição {request_id}.")
            state.finalize_request(request_id, RequestStatus.FAILED)
            return

        node_metadata = {
            res['source']: {"ip": res['ip_source'], "services": res['services']}
            for res in responses
        }
        self.logger.info(f"QUEM RESPONDEU: {node_metadata}")

        ranking = []
        for node_id, data in node_metadata.items():
            try:
                reputation = _reputation_of(node_id)["reputation"]
                ranking.append({
                    "node_id": node_id,
                    "ip_source": data['ip'],
                    "services": data['services'],
                    "reputation": reputation,
                })
            except Exception as e:
                self.logger.error(f"Erro ao processar candidato {node_id}: {e}")

        if not ranking:
            self.logger.error("Falha ao gerar ranking.")
            state.finalize_request(request_id, RequestStatus.FAILED)
            return

        ranking.sort(key=lambda x: x['reputation'], reverse=True)
        best_node = ranking[0]

        self.logger.info(
            f"Melhor nó: {best_node['node_id']} | Reputação: {best_node['reputation']} | "
            f"Serviços Oferecidos: {len(best_node['services'])}"
        )

        state.update_request_status(request_id, RequestStatus.REQUESTING_SERVICE)
        self.invoke_async(ServiceName.TASK_REQUEST_NODE_SERVICE,
                          {"request_id": request_id, "best_node": best_node}, None)

        self.response.payload = {"request_id": request_id, "best_node": best_node}


# ===========================================================================
# Consumo e avaliação
# ===========================================================================
class RequestNodeServiceTask(Service):
    """Consome o serviço do provedor por HTTP direto, avalia, atualiza credibilidade e publica na DLT."""
    name = ServiceName.TASK_REQUEST_NODE_SERVICE

    def handle(self):
        request_id = self.request.payload.get('request_id')
        best_node = self.request.payload.get('best_node')
        state = get_state()

        if not request_id or not best_node:
            self.logger.error("Falha: 'request_id' ou 'best_node' não fornecidos para consumo de dados.")
            state.finalize_request(request_id, RequestStatus.FAILED)
            return

        ip_target = best_node.get('ip_source')
        services = best_node.get('services', [])
        node_id = best_node.get('node_id')

        self.logger.info(f"Iniciando consumo direto de dados do Gateway {node_id} (IP: {ip_target})...")

        if not services:
            self.logger.warning(f"O nó {node_id} não possui dispositivos/sensores listados.")
            state.finalize_request(request_id, RequestStatus.FAILED_NO_SERVICES)
            return

        collected_data, has_error = _consume_services(ip_target, services)
        has_collected_data = len(collected_data) > 0

        # Nota bruta (fidedigna)
        if has_collected_data:
            raw_service_evaluation, raw_evaluation_value = 1, 1.0
            status = RequestStatus.FINISHED
            self.logger.info(f"[AVALIAÇÃO PROVEDOR] Dados válidos recebidos. Nota bruta: {raw_evaluation_value}")
        else:
            raw_service_evaluation, raw_evaluation_value = 0, 0.0
            status = RequestStatus.FAILED_DATA_CONSUMPTION
            self.logger.warning(
                f"[AVALIAÇÃO PROVEDOR] Falha ao consumir dados (possível falsificação). "
                f"Nota bruta reduzida para: {raw_evaluation_value}"
            )

        # Conduta do nó (pode viciar a nota ou torná-lo egoísta)
        evaluation = apply_conduct_to_evaluation(node_id, raw_service_evaluation, raw_evaluation_value)
        conduct_applied = evaluation["conduct_applied"]
        final_service_evaluation = evaluation["final_service_evaluation"]
        final_evaluation_value = evaluation["final_evaluation_value"]

        # Credibilidade do avaliador
        identity = IDManager()
        consensus_rep = best_node.get('reputation', 0.5)
        cred = _update_credibility(identity.id, node_id, final_evaluation_value, consensus_rep)
        old_cred = cred['old_credibility']
        new_cred = cred['new_credibility']

        try:
            write_message(credibility_index(identity.id),
                          build_credibility_update(identity.id, new_cred))
            self.logger.info(f"Nova credibilidade ({new_cred}) salva na Tangle para o avaliador {identity.id}.")
        except Exception as e:
            self.logger.error(f"Erro ao salvar credibilidade na Tangle: {e}")

        # Publicação da avaliação (o egoísta não publica)
        if conduct_applied != Conduct.SELFISH:
            write_message(node_id, build_evaluation(
                source=identity.id,
                group=identity.group,
                target=node_id,
                service_evaluation=final_service_evaluation,
                node_credibility=old_cred,
                value=final_service_evaluation * old_cred,
            ))
            self.logger.info(f"Avaliação publicada na Tangle para o provedor {node_id}.")

        state.update_request_evaluation_data(
            request_id=request_id,
            provider_id=node_id,
            behavior=conduct_applied,
            consistency=cred['consistency'],
            reliability=cred['reliability'],
            reputation_provider=consensus_rep,
            old_cred=old_cred,
            new_cred=new_cred,
            reputation_evaluator=state.get_reputation(),
        )
        state.finalize_request(request_id, status)

        self.response.payload = {
            "request_id": request_id,
            "status": "success" if has_collected_data else "error",
            "collected_data_count": len(collected_data),
            "partial_errors": has_error,
        }


# ===========================================================================
# Reputação própria
# ===========================================================================
class CalculateNodeReputationTask(Service):
    """Recalcula a reputação do próprio gateway a partir da Tangle e persiste no estado local."""
    name = 'soft-iot.reputation.task.calculate'

    def handle(self):
        my_id = IDManager().id
        result = _reputation_of(my_id)

        if result['status'] == 'success':
            get_state().set_reputation(result['reputation'])
            self.logger.info(f"SUCESSO: Minha reputação salva no arquivo local: {result['reputation']}")
            self.response.payload = {"status": "success", "my_current_reputation": result['reputation']}
        else:
            self.response.payload = {"status": "no_data"}