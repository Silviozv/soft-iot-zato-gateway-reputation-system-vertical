# -*- coding: utf-8 -*-

import os
import requests
import json

from zato.server.service import Service


class DLTClientBaseService(Service):
    """
    Classe base de configurações. 
    """

    def _init_configs(self):

        # Leitura do IP e da porta da API pelas variáveis de ambiente. Tem como default: localhost:3000
        self.api_ip = os.environ.get('Zato_TANGLE_API_IP', '127.0.0.1')
        self.api_port = os.environ.get('Zato_TANGLE_API_PORT', '3000')
        self.base_url = f"http://{self.api_ip}:{self.api_port}"

    def _ensure_configs(self):
        """Garante que a base_url existe antes de qualquer operação."""

        if not hasattr(self, 'base_url'):
            self._init_configs()


class DLTWriterService(DLTClientBaseService):
    """
    Serviço de escrita na Tangle.
    """

    name = 'soft-iot.dlt.client.api.write'


    def handle(self):
        """
        Escrita de mensagem.
        """

        # Extração dos dados da requisição.
        request_data = self.request.payload
        index = request_data.get('index')             # Índice.
        transaction_data = request_data.get('data')   # Dados.

        self._ensure_configs()

        # Validação básico, caso não haja dados.
        if not index or not transaction_data:
            self.logger.error("Index ou data ausentes no payload.")
            self.response.payload = {"status": "error", "message": "Missing index or data"}
            return

        url = f"{self.base_url}/message"     # Endpoint de escrita na Tangle.
        payload = {'index': index, 'data': transaction_data}

        try:
            # Fazendo chamada da rota de escrita da API.
            response = requests.post(url, json=payload, timeout=10)

            if response.status_code in (200, 201):
                self.logger.info(f"RESPOSTA DA ESCRITA: {response.text}") 
                
                # Se a API retornou um JSON válido, entrega a resposta do Zato.
                if response.text and response.text.strip():
                    try:
                        self.response.payload = {"status": "success", "tangle_res": response.json()}
                    except ValueError:
                        self.response.payload = {"status": "error"}
                else:
                    self.response.payload = {"status": "error"}

            else:
                self.logger.error(f"Erro na API Hornet ({response.status_code}): {response.text}")
                self.response.payload = {"status": "error", "code": response.status_code}
                
        except Exception as e:
            self.logger.error(f"Falha na requisição para {url}: {e}")
            self.response.payload = {"status": "error", "message": str(e)}


class DLTIndexReaderService(DLTClientBaseService):
    name = 'soft-iot.dlt.client.api.read_index'

    def handle(self):
        self._ensure_configs()
        request_data = self.request.payload or self.request.params
        index = request_data.get('index')

        if not index:
            self.logger.error("Índice não fornecido para busca.")
            self.response.payload = [] # Garante lista vazia
            return

        url = f"{self.base_url}/message/{index}"   
        
        try:
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200 and response.text.strip():
                try:
                    raw_data = response.json()
                    # Se raw_data for None ou não for lista, retorna lista vazia
                    if not isinstance(raw_data, list):
                        self.response.payload = []
                        return

                    parsed_data = []
                    for item in raw_data:
                        if isinstance(item, dict) and 'data' in item:
                            if isinstance(item['data'], str):
                                try:
                                    item['data'] = json.loads(item['data'])
                                except json.JSONDecodeError:
                                    pass
                        parsed_data.append(item)
                    self.response.payload = parsed_data
                except ValueError:
                    self.response.payload = []
            else:
                # Caso de erro HTTP ou resposta vazia
                self.response.payload = []
        except Exception as e:
            self.logger.error(f"Falha na leitura do índice {index}: {e}")
            self.response.payload = [] 


class DLTIdReaderService(DLTClientBaseService):
    """
    Busca uma transação específica pelo ID (Substitui LedgerReader.java).
    """
    name = 'soft-iot.dlt.client.api.read_id'

    def handle(self):
        """
        Busca mensagem por ID.
        """
        
        self._ensure_configs()

        request_data = self.request.payload or self.request.params
        message_id = request_data.get('message_id')     # Coleta do ID da mensagem

        if not message_id:
            self.logger.error("Message ID não fornecido para busca.")
            self.response.payload = None
            return

        # Endpoint de busca da transação pelo ID.
        url = f"{self.base_url}/message/messageId/{message_id}"
        
        try:
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                if response.text and response.text.strip():
                    try:
                        raw_data = response.json()
                        
                        # Verifica se é um dicionário e possui o campo 'data'
                        if isinstance(raw_data, dict) and 'data' in raw_data:
                            if isinstance(raw_data['data'], str):
                                try:
                                    raw_data['data'] = json.loads(raw_data['data'])
                                except json.JSONDecodeError:
                                    self.logger.warning("Não foi possível decodificar o campo 'data' como JSON.")
                                    
                        self.response.payload = raw_data

                    except ValueError:
                        self.logger.warning(f"Resposta JSON inválida para o ID: {message_id}")
                        self.response.payload = None
                else:
                    self.response.payload = None
            else:
                self.logger.error(f"Erro na API Hornet ({response.status_code}) ao buscar ID: {message_id}")
                self.response.payload = None

        except Exception as e:
            self.logger.error(f"Falha na requisição de busca por ID {message_id}: {e}")
            self.response.payload = None
