# -*- coding: utf-8 -*-

import os
import json
import datetime
import random
import time
import math
import requests

from datetime import datetime, timezone

from zato.server.service import Service

from soft_iot_behavior import NodeTypeManager
from soft_iot_state import GatewayStateManager


class GetGatewayStateService(Service):
    """
    Serviço de leitura (Read Endpoint) que expõe o estado atual do Gateway.
    Recupera os dados persistidos no banco local e os serializa num Data Transfer Object (DTO).
    """
    name = 'soft-iot.gateway.state.get'

    def handle(self):
        self.logger.info("Executando GetGatewayStateService para extração completa de estado local...")
        
        try:
            # Instancia o DAO
            gs_manager = GatewayStateManager()
            
            # Coleta a totalidade dos dados das tabelas
            properties_data = gs_manager.get_all_properties()
            requests_data = gs_manager.get_all_requests()
            responses_data = gs_manager.get_all_responses() 
            
            # Monta o DTO hierárquico para a resposta (Payload)
            self.response.payload = {
                "status": "success",
                "data": {
                    "gateway_properties": properties_data,
                    "requests_history": requests_data,
                    "responses_history": responses_data, # Respostas integradas no payload
                    "metrics": {
                        "total_requests_logged": len(requests_data),
                        "total_responses_logged": len(responses_data) # Nova métrica
                    }
                }
            }
            
            self.logger.info(
                f"Dados exportados com sucesso. Requisições: {len(requests_data)} | Respostas: {len(responses_data)}"
            )
            
        except Exception as e:
            self.logger.error(f"Erro crítico ao recuperar o estado do gateway: {e}")
            self.response.payload = {
                "status": "error",
                "message": "Falha interna ao ler a base de dados local."
            }

           
class CalculateNodeReputationTask(Service):
    """
    Calcula a reputação do próprio Gateway baseado nas avaliações da Tangle.
    """
    name = 'soft-iot.reputation.task.calculate'

    def handle(self):
        self.logger.info("Executando CalculateNodeReputationTask...")

        identity_info = self.invoke('soft-iot.id.manager')
        my_gateway_id = identity_info.get('gateway_id')

        gs_manager = GatewayStateManager()

        if not my_gateway_id:
            # Retorna o valor sob demanda (a escrita desnecessária de consenso na Tangle foi removida)
            self.response.payload = {"status": "error","message": "no_id"}
            return

        # Calcula a própria reputação
        rep_result = self.invoke('soft-iot.reputation.orchestrator', {'node_id': my_gateway_id})

        if rep_result.get('status') == 'success':
            my_current_reputation = rep_result.get('reputation')
            
            gs_manager.set_reputation(my_current_reputation)
            
            self.logger.info(f"SUCESSO: Minha reputação salva no arquivo local: {my_current_reputation}")

            self.response.payload = {"status": "success", "my_current_reputation": my_current_reputation}
    

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
        

class CheckNodesServicesTask(Service):
    """
    Sorteia um serviço de interesse e publica uma requisição de busca (Broadcast) na Tangle.
    """
    name = 'soft-iot.reputation.task.check_nodes_services'

    def handle(self):
        self.logger.info("Executando CheckNodesServicesTask (Requisição de Serviços)...")

        gs_manager = GatewayStateManager()

        last_id, last_status = gs_manager.get_last_request_status()

        # Bloqueio de Concorrência: Verifica se já existe um pedido em andamento
        if last_status == "WAITING_RESPONSES"  or last_status == "CHOSING_BETTER_NODE" or last_status == "REQUESTING_SERVICE":

            start_time_str = gs_manager.get_start_request_time(last_id)

            if start_time_str:
            
                start_time_naive = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
                start_time_aware = start_time_naive.replace(tzinfo=timezone.utc)
                current_time_aware = datetime.now(timezone.utc)
                time_difference = current_time_aware - start_time_aware

                if time_difference.total_seconds() > 120:
                    self.logger.warning(f"TIMEOUT: A requisição {last_id} excedeu o limite com status {last_status}!")
                    
                    gs_manager.finalize_request(last_id, 'TIMEOUT')
                    
                else:
                    self.logger.info(f"Requisição {last_id} em andamento.")
                    self.response.payload = {"status": "Already requesting"}
                    return
        
        # Lista fixa de serviços e cálculo de escolha aleatória
        available_services = [
            "Thermometer",
            "HumiditySensor",
            "PulseOxymeter",
            "WindDirectionSensor"
        ]

        chosen_service = random.choice(available_services)
        self.logger.info(f"Serviço escolhido aleatoriamente para requisição: {chosen_service}")
    
        # Recolhe a identidade do Gateway local
        identity_info = self.invoke('soft-iot.id.manager')
        my_gateway_id = identity_info.get('gateway_id')
        my_group = identity_info.get('tangle_group')

        if not my_gateway_id:
            self.logger.error("Falha: ID do Gateway não configurado.")
            self.response.payload = {"status": "ID not found"}
            return 
        
        request_id = gs_manager.create_request(my_gateway_id)


        # Estrutura do payload da requisição 
        request_transaction = {
            "source": my_gateway_id,
            "group": my_group,
            "type": "REP_SVC_REQ",
            "requestedService": chosen_service,
            "idRequest": request_id
        }

        # Publicação na Tangle utilizando o índice acordado para requisições
        broadcast_index = 'REP_HAS_SVC'
        
        try:
            res = self.invoke('soft-iot.dlt.client.api.write', {
                "index": broadcast_index,
                "data": request_transaction
            })

            if res.get("status") == "success":
                self.logger.info(f"Requisição do serviço {chosen_service} publicada no índice '{broadcast_index}'.")
            
                # Dispara o loop de contagem no Zato de forma assíncrona (não bloqueia esta task)
                self.invoke_async('soft-iot.reputation.task.wait_nodes_responses', {'request_id': request_id}, None)

                self.response.payload = {"status": "Request created"}
                return 
            
            else:
                self.logger.warning(f"Falha na rede ao publicar requisição: {res}")
                gs_manager.finalize_request(request_id, 'FAILED')
                self.response.payload = {"status": "Tangle write failed", "tangle_response": res}
                return
                
        except Exception as e:
            self.logger.error(f"Erro crítico ao solicitar serviço: {e}")
            gs_manager.finalize_request(request_id, 'FAILED')
            self.response.payload = {"status": "Internal Error"}
            return

class WaitNodesResponsesTask(Service):
    """
    Tarefa assíncrona que mantém uma janela de tempo aberta para coleta de propostas (respostas).
    Após o término, avança a máquina de estados para que o Gateway escolha o melhor nó.
    """
    name = 'soft-iot.reputation.task.wait_nodes_responses'

    def handle(self):
        # Recupera os dados enviados pela CheckNodesServicesTask
        payload = self.request.payload or {}
        request_id = payload.get('request_id')
        
        # Define o tempo da janela de coleta (ex: 15 segundos)
        wait_time = payload.get('wait_time', 10) 

        if not request_id:
            self.logger.error("Falha Crítica: Nenhum ID de requisição fornecido para a tarefa de espera.")
            self.response.payload = {"status": "error", "message": "Missing ID"}
            return

        self.logger.info(f"Iniciando janela de coleta de {wait_time}s para a requisição {request_id}...")

        # Bloqueia este Worker de segundo plano (Deixa o banco recebendo as respostas no servidor ZMQ)
        time.sleep(wait_time)

        # Fim da espera: Verifica se a requisição ainda é válida
        gs_manager = GatewayStateManager()
        current_status = gs_manager.get_request_status(request_id)

        # Só avança o estado se a transação não sofreu um Timeout absoluto e não falhou
        if current_status == 'WAITING_RESPONSES':
            self.logger.info(f"Janela concluída. Atualizando status da requisição {request_id} para iniciar cálculo.")
            
            # Muda o status
            gs_manager.update_request_status(request_id, 'CHOSING_BETTER_NODE')
            
            self.invoke_async('soft-iot.reputation.task.select_best_node', {"request_id": request_id}, None)
            
        else:
            self.logger.warning(
                f"A requisição {request_id} mudou de estado prematuramente "
                f"(Status atual: {current_status}). Abortando transição."
            )

class SelectBestNodeTask(Service):
    """
    Task que analisa candidatos, calcula reputação e seleciona o melhor nó.
    """
    name = 'soft-iot.reputation.task.select_best_node'

    def handle(self):
        request_id = self.request.payload.get('request_id')
        
        if not request_id:
            self.logger.error("request_id não fornecido para SelectBestNodeTask.")
            return

        self.logger.info(f"Iniciando análise de candidatos para a requisição {request_id}...")
        
        gs_manager = GatewayStateManager()
        responses = gs_manager.get_responses_for_request(request_id)
        
        if not responses:
            self.logger.warning(f"Nenhuma resposta recebida para a requisição {request_id}.")
            gs_manager.finalize_request(request_id, 'FAILED')
            return

        # Estruturando dados das respostas
        node_metadata = {}
        
        for res in responses:
            node_id = res['source']
            ip_source = res['ip_source']
            services = res['services']
            
            node_metadata[node_id] = {
                "ip": ip_source,
                "services": services
            }

        self.logger.info(f"QUEM RESPONDEU: {node_metadata}...")

        candidates_ranking = []

        # Calcula a reputação para cada nó único
        for node_id, data in node_metadata.items():
            try:
                self.logger.info(f"Calculando reputação para: {node_id}")
                
                rep_res = self.invoke('soft-iot.reputation.orchestrator', {'node_id': node_id})
                
                # Fallback de segurança para 0.5
                if rep_res and (rep_res.get('status') == 'success' or rep_res.get('status') == 'no_data'):
                    reputation_score = rep_res.get('reputation', 0.5)
                else:
                    reputation_score = 0.5
                
                candidates_ranking.append({
                    "node_id": node_id,
                    "ip_source": data['ip'],
                    "services": data['services'], 
                    "reputation": reputation_score
                })
                    
            except Exception as e:
                self.logger.error(f"Erro ao processar candidato {node_id}: {e}")

        self.logger.info(f"RANKING: {node_metadata}...")

        # Ordenação: Maior reputação primeiro
        candidates_ranking.sort(key=lambda x: x['reputation'], reverse=True)

        if not candidates_ranking:
            self.logger.error("Falha ao gerar ranking.")
            gs_manager.finalize_request(request_id, 'FAILED')
            return

        # Seleção do Vencedor
        best_node = candidates_ranking[0]
        
        self.logger.info(
            f"Melhor nó: {best_node['node_id']} | Reputação: {best_node['reputation']} | "
            f"Serviços Oferecidos: {len(best_node['services'])}"
        )

        gs_manager.update_request_status(request_id, 'REQUESTING_SERVICE')

        self.invoke_async('soft-iot.reputation.task.request_node_service', {"request_id": request_id, "best_node": best_node}, None)

        self.response.payload = {
            "request_id": request_id,
            "best_node": best_node
        }

class RequestNodeServiceTask(Service):
    """
    Task que realiza a requisição HTTP direta (Ponto-a-Ponto) para o Gateway provedor,
    consumindo os dados do sensor solicitado sem passar pela Tangle.
    """
    name = 'soft-iot.reputation.task.request_node_service'

    def handle(self):

        request_id = self.request.payload.get('request_id')
        best_node = self.request.payload.get('best_node')

        gs_manager = GatewayStateManager()
        
        if not request_id or not best_node:
            self.logger.error("Falha: 'request_id' ou 'best_node' não fornecidos para consumo de dados.")
            gs_manager.finalize_request(request_id, 'FAILED')
            return
        
        ip_target = best_node.get('ip_source')
        services = best_node.get('services', [])
        node_id = best_node.get('node_id')
        
        self.logger.info(f"Iniciando consumo direto de dados do Gateway {node_id} (IP: {ip_target})...")

        # Verifica se o nó ofereceu algum serviço válido
        if not services:
            self.logger.warning(f"O nó {node_id} não possui dispositivos/sensores listados.")
            gs_manager.finalize_request(request_id, 'FAILED_NO_SERVICES')
            return

        collected_data = []
        has_error = False

        # Itera sobre os serviços oferecidos e faz a requisição REST
        # (Um gateway pode ter oferecido múltiplos sensores compatíveis)
        for service in services:
            device_id = service.get('device_id')
            sensor_id = service.get('sensor_id')
            
            url = f"http://{ip_target}:11223/soft-iot/devices/{device_id}/sensors/{sensor_id}/data/latest"

            
            try:
                self.logger.info(f"Requisitando dados: {url}")
                
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    # Converte a string JSON para Dicionário Python
                    json_response = response.json()
                    
                    # Extrai especificamente o "item" de dentro da chave "data", 
                    # de acordo com o seu serviço GetLastData
                    sensor_item = json_response.get('data')
                    
                    self.logger.info(f"Dado recebido com sucesso de {device_id}/{sensor_id}: {sensor_item}")
                    
                    collected_data.append({
                        "device": device_id,
                        "sensor": sensor_id,
                        "data": sensor_item # Salva apenas a métrica real
                    })
                else:
                    # Se o status_code for 500, o seu serviço GetLastData retorna {'error': str(e)}
                    # Podemos capturar isso para colocar no log
                    error_msg = response.json().get('error', 'Erro desconhecido')
                    self.logger.warning(
                        f"Falha ao obter dado de {device_id}/{sensor_id}. "
                        f"Status: {response.status_code}. Erro do nó: {error_msg}"
                    )
                    has_error = True
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Erro de rede ao tentar acessar o IP {ip_target}: {e}")
                has_error = True

        

        has_collected_data = len(collected_data) > 0

        identity_info = self.invoke('soft-iot.id.manager')
        id_evaluator = identity_info.get('gateway_id')
        group_evaluator = identity_info.get('tangle_group')

        if has_collected_data:
            raw_service_evaluation = 1
            raw_evaluation_value = 1.0
            status = 'FINISHED'
            self.logger.info(f"[AVALIAÇÃO PROVEDOR] Dados válidos recebidos. Nota bruta: {raw_evaluation_value}")
        else:
            raw_service_evaluation = 0
            raw_evaluation_value = 0.0
            status = 'FAILED_DATA_CONSUMPTION'
            self.logger.warning(f"[AVALIAÇÃO PROVEDOR] Falha ao consumir dados (possível falsificação). Nota bruta reduzida para: {raw_evaluation_value}")

        self.logger.info(f"O status será: {status}.")


        self.logger.info(f"Nota bruta gerada: {raw_evaluation_value}")

        # Decidindo conduta da avaliação
        eval_res = self.invoke('soft-iot.node.evaluation', {
            "provider_id": node_id,
            "serviceEvaluation": raw_service_evaluation,
            "value": raw_evaluation_value
        })

        # Recupera as notas (que podem ter sido viciadas por um ataque malicioso)
        conduct_applied = eval_res.get('conduct_applied', 'HONEST')
        final_service_evaluation = eval_res.get('final_service_evaluation', raw_service_evaluation)
        final_evaluation_value = eval_res.get('final_evaluation_value', raw_evaluation_value)

        consensus_rep = best_node.get('reputation', 0.5)

        cred_res = self.invoke('soft-iot.reputation.credibility.manager', {
            "evaluator_id": id_evaluator,
            "provider_id": node_id,
            "evaluation_given": final_evaluation_value,
            "consensus_reputation": consensus_rep
        })

        old_cred = cred_res.get('old_credibility', 0.5)
        new_cred = cred_res.get('new_credibility', 0.5)
        reliability = cred_res.get('reliability', 1.0)
        consistency = cred_res.get('consistency', 1.0)

        # Atualização da credibilidade do nó avaliador
        try:
            current_timestamp = int(time.time())

            cred_transaction = {
                "type": "CRED_UPDATE",
                "evaluator_id": id_evaluator,
                "credibility": float(new_cred),
                "timestamp": current_timestamp
            }
            
            self.invoke('soft-iot.dlt.client.api.write', {
                "index": f"CRED_{id_evaluator}",
                "data": cred_transaction
            })
            self.logger.info(f"Nova credibilidade ({new_cred}) salva na Tangle para o avaliador {id_evaluator}.")
        except Exception as e:
            self.logger.error(f"Erro ao salvar credibilidade na Tangle: {e}")

        # Publicação na Tangle. Ignora se for egoísta
        if conduct_applied != 'SELFISH':

            current_timestamp = int(time.time())

            final_evaluation_value_with_cred = final_service_evaluation * old_cred

            evaluation_transaction = {
                "source": id_evaluator,
                "group": group_evaluator,
                "type": "REP_EVALUATION",
                "target": node_id,
                "serviceEvaluation": final_service_evaluation,
                "nodeCredibility": float(old_cred),
                "value": final_evaluation_value_with_cred,
                "timestamp": current_timestamp
            }
            
            self.invoke('soft-iot.dlt.client.api.write', {
                "index": node_id,
                "data": evaluation_transaction
            })
            self.logger.info(f"Avaliação publicada na Tangle para o provedor {node_id}.")
        
        self.logger.info(f"Coletando reputação.")

        my_current_reputation = gs_manager.get_reputation()

        self.logger.info(f"Atualizando requisição.")

        gs_manager.update_request_evaluation_data(
            request_id=request_id,
            provider_id=node_id,
            behavior=conduct_applied,
            consistency=consistency,
            reliability=reliability,
            reputation_provider=consensus_rep,
            old_cred=old_cred,
            new_cred=new_cred,
            reputation_evaluator=my_current_reputation
        )

        self.logger.info(f"Finalizando requisição.")

        gs_manager.finalize_request(request_id, status)

        self.response.payload = {
            "request_id": request_id,
            "status": "success" if has_collected_data else "error",
            "collected_data_count": len(collected_data),
            "partial_errors": has_error
        }
