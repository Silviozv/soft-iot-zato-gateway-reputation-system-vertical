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

from soft_iot_node_type import NodeTypeManager
from soft_iot_gateway_state import GatewayStateManager

# Pesos da reputação conjunta da dupla gateway/device (50-50)
GATEWAY_REPUTATION_WEIGHT = 0.5
DEVICE_REPUTATION_WEIGHT = 0.5


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
        gs_manager.set_requested_service(request_id, chosen_service)


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
                self.invoke_async('soft-iot.reputation.task.wait_nodes_responses', {'request_id': request_id, 'requested_service': chosen_service}, None)

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
        requested_service = payload.get('requested_service')
        
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
            
            self.invoke_async('soft-iot.reputation.task.select_best_node', {"request_id": request_id, "requested_service": requested_service}, None)
            
        else:
            self.logger.warning(
                f"A requisição {request_id} mudou de estado prematuramente "
                f"(Status atual: {current_status}). Abortando transição."
            )

class SelectBestNodeTask(Service):
    """
    Task que analisa candidatos e seleciona a melhor dupla gateway/device.
    Para cada gateway que respondeu, consulta na Tangle os devices com o serviço pedido,
    calcula a reputação do gateway e de cada device e usa a reputação conjunta (50-50).
    """
    name = 'soft-iot.reputation.task.select_best_node'

    def handle(self):
        request_id = self.request.payload.get('request_id')
        requested_service = self.request.payload.get('requested_service')
        
        if not request_id:
            self.logger.error("request_id não fornecido para SelectBestNodeTask.")
            return

        self.logger.info(f"Iniciando análise de candidatos para a requisição {request_id} (serviço: {requested_service})...")
        
        gs_manager = GatewayStateManager()
        responses = gs_manager.get_responses_for_request(request_id)
        
        if not responses:
            self.logger.warning(f"Nenhuma resposta recebida para a requisição {request_id}.")
            gs_manager.finalize_request(request_id, 'FAILED')
            return

        # Estruturando dados das respostas (um registro por gateway)
        node_metadata = {}
        
        for res in responses:
            node_id = res['source']
            ip_source = res['ip_source']
            
            node_metadata[node_id] = {
                "ip": ip_source
            }

        self.logger.info(f"QUEM RESPONDEU: {node_metadata}...")

        candidates_ranking = []

        for node_id, data in node_metadata.items():
            try:
                # Devices do gateway que possuem o serviço (consulta na Tangle)
                devices = self.invoke('soft-iot.dlt.gateway.devices', {
                    'gateway_id': node_id,
                    'service': requested_service
                })

                if not isinstance(devices, list) or not devices:
                    self.logger.warning(f"Gateway {node_id} não possui devices registrados na Tangle com o serviço '{requested_service}'.")
                    continue

                # Reputação do gateway
                self.logger.info(f"Calculando reputação para: {node_id}")
                rep_res = self.invoke('soft-iot.reputation.orchestrator', {'node_id': node_id})
                
                # Fallback de segurança para 0.5
                if rep_res and (rep_res.get('status') == 'success' or rep_res.get('status') == 'no_data'):
                    gateway_reputation = rep_res.get('reputation', 0.5)
                else:
                    gateway_reputation = 0.5

                # Reputação de cada device e reputação conjunta da dupla
                for device in devices:
                    device_id = device.get('device_id')
                    sensor_id = device.get('sensor_id')

                    dev_res = self.invoke('soft-iot.reputation.device', {
                        'device_id': device_id,
                        'gateway_id': node_id
                    })

                    if dev_res and (dev_res.get('status') == 'success' or dev_res.get('status') == 'no_data'):
                        device_reputation = dev_res.get('reputation', 0.5)
                    else:
                        device_reputation = 0.5

                    joint_reputation = (GATEWAY_REPUTATION_WEIGHT * gateway_reputation) + \
                                       (DEVICE_REPUTATION_WEIGHT * device_reputation)

                    candidates_ranking.append({
                        "node_id": node_id,
                        "ip_source": data['ip'],
                        "device_id": device_id,
                        "sensor_id": sensor_id,
                        "requested_service": requested_service,
                        "reputation": gateway_reputation,          # Reputação do gateway (usada na credibilidade)
                        "device_reputation": device_reputation,
                        "joint_reputation": round(joint_reputation, 4)
                    })
                    
            except Exception as e:
                self.logger.error(f"Erro ao processar candidato {node_id}: {e}")

        self.logger.info(f"RANKING: {candidates_ranking}...")

        # Ordenação: Maior reputação conjunta primeiro
        candidates_ranking.sort(key=lambda x: x['joint_reputation'], reverse=True)

        if not candidates_ranking:
            self.logger.error("Falha ao gerar ranking (nenhuma dupla gateway/device disponível).")
            gs_manager.finalize_request(request_id, 'FAILED_NO_SERVICES')
            return

        # Seleção do Vencedor
        best_node = candidates_ranking[0]
        
        self.logger.info(
            f"Melhor dupla: gateway {best_node['node_id']} (rep {best_node['reputation']}) / "
            f"device {best_node['device_id']} (rep {best_node['device_reputation']}) | "
            f"Reputação conjunta: {best_node['joint_reputation']}"
        )

        gs_manager.update_request_status(request_id, 'REQUESTING_SERVICE')

        self.invoke_async('soft-iot.reputation.task.request_node_service', {"request_id": request_id, "best_node": best_node}, None)

        self.response.payload = {
            "request_id": request_id,
            "best_node": best_node
        }

class RequestNodeServiceTask(Service):
    """
    Task que realiza a requisição HTTP direta (Ponto-a-Ponto) para a dupla gateway/device escolhida,
    consumindo o dado do sensor solicitado sem passar pela Tangle.
    Apenas o gateway provedor é avaliado.
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
        node_id = best_node.get('node_id')
        device_id = best_node.get('device_id')
        sensor_id = best_node.get('sensor_id')
        
        self.logger.info(f"Iniciando consumo direto de dados do Gateway {node_id} (IP: {ip_target}) / Device {device_id}...")

        if not device_id or not sensor_id:
            self.logger.warning(f"Dupla escolhida sem device/sensor (gateway {node_id}).")
            gs_manager.finalize_request(request_id, 'FAILED_NO_SERVICES')
            return

        url = f"http://{ip_target}:11223/soft-iot/provider/devices/{device_id}/sensors/{sensor_id}/data/latest"

        received_value = None
        has_received_data = False

        try:
            self.logger.info(f"Requisitando dados: {url}")
            
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                sensor_item = response.json().get('data') or {}
                received_value = sensor_item.get('data_value')
                has_received_data = True
                self.logger.info(f"Dado recebido de {device_id}/{sensor_id}: {sensor_item}")
            else:
                try:
                    error_msg = response.json().get('error', 'Erro desconhecido')
                except ValueError:
                    error_msg = response.text
                self.logger.warning(
                    f"Falha ao obter dado de {device_id}/{sensor_id}. "
                    f"Status: {response.status_code}. Erro do nó: {error_msg}"
                )
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Erro de rede ao tentar acessar o IP {ip_target}: {e}")

        # Nota bruta: 1 para dado válido, 0 para dado inválido ou falha no consumo
        # Import local: o Zato carrega os arquivos em ordem alfabética, e este módulo
        # vem antes dos que ele usa. No momento da chamada, todos já estão carregados.
        from soft_iot_device_trust import is_valid_reading

        is_valid_data = has_received_data and is_valid_reading(received_value)

        identity_info = self.invoke('soft-iot.id.manager')
        id_evaluator = identity_info.get('gateway_id')
        group_evaluator = identity_info.get('tangle_group')

        if is_valid_data:
            raw_service_evaluation = 1
            raw_evaluation_value = 1.0
            status = 'FINISHED'
            self.logger.info(f"[AVALIAÇÃO PROVEDOR] Dado válido recebido ({received_value}). Nota bruta: {raw_evaluation_value}")
        elif has_received_data:
            raw_service_evaluation = 0
            raw_evaluation_value = 0.0
            status = 'FINISHED_INVALID_DATA'
            self.logger.warning(f"[AVALIAÇÃO PROVEDOR] Dado inválido recebido ({received_value}). Nota bruta: {raw_evaluation_value}")
        else:
            raw_service_evaluation = 0
            raw_evaluation_value = 0.0
            status = 'FAILED_DATA_CONSUMPTION'
            self.logger.warning(f"[AVALIAÇÃO PROVEDOR] Falha ao consumir dados. Nota bruta: {raw_evaluation_value}")

        self.logger.info(f"O status será: {status}.")

        # Decidindo conduta da avaliação
        eval_res = self.invoke('soft-iot.node.evaluation', {
            "provider_id": node_id,
            "serviceEvaluation": raw_service_evaluation,
            "value": raw_evaluation_value
        })

        # Recupera as notas (que podem ter sido invertidas por um ataque malicioso)
        conduct_applied = eval_res.get('conduct_applied', 'HONEST')
        final_service_evaluation = eval_res.get('final_service_evaluation', raw_service_evaluation)
        final_evaluation_value = eval_res.get('final_evaluation_value', raw_evaluation_value)

        consensus_rep = best_node.get('reputation', 0.5)

        old_cred = None
        new_cred = None
        reliability = None
        consistency = None

        # Egoísta consome o dado, mas não avalia nem ajusta a credibilidade (sem registros na Tangle)
        if conduct_applied != 'SELFISH':

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

            # Registro da nota do gateway provedor na Tangle
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

            # Atualização da credibilidade do nó avaliador
            try:
                cred_transaction = {
                    "type": "CRED_UPDATE",
                    "evaluator_id": id_evaluator,
                    "credibility": float(new_cred),
                    "timestamp": int(time.time())
                }
                
                self.invoke('soft-iot.dlt.client.api.write', {
                    "index": f"CRED_{id_evaluator}",
                    "data": cred_transaction
                })
                self.logger.info(f"Nova credibilidade ({new_cred}) salva na Tangle para o avaliador {id_evaluator}.")
            except Exception as e:
                self.logger.error(f"Erro ao salvar credibilidade na Tangle: {e}")

        else:
            self.logger.info(f"Nó egoísta: avaliação de {node_id} e ajuste de credibilidade não realizados.")
        
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

        gs_manager.update_request_device_data(
            request_id=request_id,
            device_id=device_id,
            sensor_id=sensor_id,
            reputation_device=best_node.get('device_reputation'),
            reputation_joint=best_node.get('joint_reputation'),
            received_data_value=received_value
        )

        self.logger.info(f"Finalizando requisição.")

        gs_manager.finalize_request(request_id, status)

        self.response.payload = {
            "request_id": request_id,
            "status": "success" if is_valid_data else "error",
            "received_data_value": received_value
        }
