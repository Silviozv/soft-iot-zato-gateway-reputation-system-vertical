# -*- coding: utf-8 -*-

import sqlite3
import logging
import json
from contextlib import closing

class GatewayStateManager:
    """
    Data Access Object (DAO) para gerir o estado concorrente do Gateway.
    Infraestrutura base com suporte a Write-Ahead Logging (WAL).
    """

    _STATE_KEY = 'gateway_state'

    def __init__(self, db_path='/tmp/gateway_state.db'):
        self.db_path = db_path
        self.logger = logging.getLogger('zato.gateway.state')
        # Garante que a base de dados existe assim que o módulo é instanciado
        self._init_db()

    def _get_connection(self):
        """
        Estabelece a conexão com o banco de dados SQLite.
        """
        # O parâmetro timeout=10.0 obriga o Worker a aguardar até 10 segundos 
        # caso o banco esteja momentaneamente trancado por outra transação, 
        # em vez de falhar imediatamente.
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        
        # Ativação do modo WAL para concorrência de múltiplos processos
        conn.execute('pragma journal_mode=wal')
        
        return conn

    def _init_db(self):
        """
        Garante a criação das estruturas transacionais no disco e a inicialização do estado padrão.
        """
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    
                    # DDL: Criação da Tabela
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS gateway_properties (
                            key TEXT PRIMARY KEY,
                            reputation TEXT,
                            behavior_changed TEXT,
                            started_experiment_time DATETIME

                        )
                    ''')

                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS requests (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            id_evaluator TEXT,
                            id_provider TEXT,
                            behavior TEXT,
                            consistency REAL,
                            reliability REAL,
                            reputation_provider REAL,
                            old_credibility_evaluator REAL,
                            new_credibility_evaluator REAL,
                            start_request_time DATETIME,
                            finish_request_time DATETIME,
                            reputation_evaluator REAL,
                            status TEXT
                        )
                    ''')

                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS responses (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            id_request INTEGER,
                            source TEXT,
                            ip_source TEXT,
                            target TEXT,
                            services TEXT, -- Armazenado como JSON String
                            group_name TEXT,
                            FOREIGN KEY (id_request) REFERENCES requests (id)
                        )
                    ''')
                    
                    # Inicializa a linha única com a chave fixa
                    cursor.execute('''
                        INSERT OR IGNORE INTO gateway_properties (key, reputation, behavior_changed, started_experiment_time)
                        VALUES (?, '0.5', 'False', CURRENT_TIMESTAMP)
                    ''', (self._STATE_KEY,))
                    
                    conn.commit()

        except Exception as e:
            self.logger.error(f"Erro crítico ao inicializar a base de dados de estado: {e}")
    
    # ==========================================
    # GESTÃO DA REPUTAÇÃO (FLOAT)
    # ==========================================

    def set_reputation(self, reputation_value):
        """Atualiza a reputação na linha fixa."""

        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO gateway_properties (key, reputation) 
                        VALUES (?, ?)
                        ON CONFLICT(key) 
                        DO UPDATE SET reputation = excluded.reputation
                    ''', (self._STATE_KEY, str(float(reputation_value))))
                    conn.commit()
        except Exception as e:
            self.logger.error(f"Erro ao salvar reputação: {e}")

    def get_reputation(self):
        """Coleta a reputação e retorna como Float."""

        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT reputation FROM gateway_properties WHERE key = ?', (self._STATE_KEY,))
                    row = cursor.fetchone()
                    return float(row[0]) if row and row[0] is not None else 0.5
        except Exception as e:
            self.logger.error(f"Erro ao ler reputação: {e}")
            return 0.5

    # ==========================================
    # GESTÃO DE MUDANÇA DE CONDUTA (BOOLEAN)
    # ==========================================

    def set_behavior_changed(self, changed_status):
        """Atualiza a flag de conduta na linha fixa."""
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    str_status = str(bool(changed_status))
                    cursor.execute('''
                        INSERT INTO gateway_properties (key, behavior_changed) 
                        VALUES (?, ?)
                        ON CONFLICT(key) 
                        DO UPDATE SET behavior_changed = excluded.behavior_changed
                    ''', (self._STATE_KEY, str_status))
                    conn.commit()
        except Exception as e:
            self.logger.error(f"Erro ao salvar behavior_changed: {e}")

    def get_behavior_changed(self):
        """Coleta a flag e retorna como Boolean."""
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT behavior_changed FROM gateway_properties WHERE key = ?', (self._STATE_KEY,))
                    row = cursor.fetchone()
                    return row[0] == 'True' if row and row[0] is not None else False
        except Exception as e:
            self.logger.error(f"Erro ao ler behavior_changed: {e}")
            return False
        
    # ==========================================
    # GESTÃO DE TEMPO DE EXPERIMENTO
    # ==========================================

    def get_started_experiment_time(self):
        """Coleta a data e hora de início do experimento."""
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT started_experiment_time FROM gateway_properties WHERE key = ?', (self._STATE_KEY,))
                    row = cursor.fetchone()
                    
                    # Retorna a string do timestamp (ex: '2026-05-07 13:56:00')
                    return row[0] if row and row[0] is not None else None
        except Exception as e:
            self.logger.error(f"Erro ao ler started_experiment_time: {e}")
            return None
        

    # ==========================================
    # GESTÃO DE REQUISIÇÕES (AUDITORIA)
    # ==========================================

    def create_request(self, id_evaluator):
        """
        Cria um novo registro de requisição na tabela de auditoria.
        Inicializa apenas o id_evaluator e o status.
        Retorna o ID gerado pelo autoincremento.
        """
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    
                    # O ID é omitido para que o SQLite use o AUTOINCREMENT
                    # O status é fixado como 'WAITING_RESPONSES' conforme o requisito
                    cursor.execute('''
                        INSERT INTO requests (id_evaluator, status, start_request_time) 
                        VALUES (?, 'WAITING_RESPONSES', CURRENT_TIMESTAMP)
                    ''', (id_evaluator,))
                    
                    # Recupera o ID da linha recém-criada
                    request_id = cursor.lastrowid
                    
                    conn.commit()
                    
                    self.logger.info(f"Nova requisição de auditoria criada. ID: {request_id}")
                    return request_id
                
        except Exception as e:
            self.logger.error(f"Erro ao criar registro de requisição na tabela requests: {e}")
            return None
        
    def get_last_request_status(self):
        """
        Consulta Heurística: Retorna o status da última requisição inserida na tabela.
        Útil para saber o estado global mais recente do Gateway.
        Retorna uma tupla (id, status) ou (None, None) se a tabela estiver vazia.
        """
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    # Busca estritamente a última linha ordenada pelo autoincremento
                    cursor.execute('SELECT id, status FROM requests ORDER BY id DESC LIMIT 1')
                    row = cursor.fetchone()
                    
                    if row:
                        return row[0], row[1]
                    return None, None
                
        except Exception as e:
            self.logger.error(f"Erro ao buscar o status da última requisição: {e}")
            return None, None
        
    def get_start_request_time(self, request_id):
        """
        Consulta Determinística: Retorna o timestamp de início de uma requisição específica.
        Retorna a string do formato DATETIME (ex: '2026-05-07 14:30:00') ou None se falhar.
        """
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    
                    # Busca estritamente a coluna start_request_time baseada no ID único
                    cursor.execute('SELECT start_request_time FROM requests WHERE id = ?', (request_id,))
                    row = cursor.fetchone()
                    
                    # Verifica se a linha existe e se o valor não é nulo
                    if row and row[0] is not None:
                        return row[0]
                    
                    return None
                
        except Exception as e:
            self.logger.error(f"Erro ao buscar o tempo de início para o ID {request_id}: {e}")
            return None
    
    def update_request_status(self, request_id, new_status):
        """
        Atualiza estritamente a coluna de status de uma requisição específica.
        Retorna True se a atualização for bem-sucedida e a linha existir, False caso contrário.
        """
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    
                    # Executa a atualização focada apenas na coluna status
                    cursor.execute('''
                        UPDATE requests 
                        SET status = ? 
                        WHERE id = ?
                    ''', (new_status, request_id))
                    
                    conn.commit()
                    
                    # O rowcount verifica quantas linhas foram fisicamente alteradas no disco
                    if cursor.rowcount > 0:
                        self.logger.info(f"Status da requisição {request_id} atualizado para '{new_status}'.")
                        return True
                    else:
                        self.logger.warning(f"Tentativa de atualizar status falhou: ID {request_id} não encontrado.")
                        return False
                    
        except Exception as e:
            self.logger.error(f"Erro ao atualizar status da requisição {request_id}: {e}")
            return False
        
    def get_request_status(self, request_id):
        """
        Consulta Determinística: Retorna o status de uma requisição específica.
        Obrigatório se a decisão lógica da Task depender do fim de um ciclo exato.
        Retorna apenas a string do status.
        """
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT status FROM requests WHERE id = ?', (request_id,))
                    row = cursor.fetchone()
                    
                    if row:
                        return row[0]
                    return None
                
        except Exception as e:
            self.logger.error(f"Erro ao buscar o status para o ID {request_id}: {e}")
            return None
        
    def finalize_request(self, request_id, final_status):
        """
        Encerra o ciclo de uma requisição, gravando o status final 
        e o timestamp exato da conclusão.
        """
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    
                    # Atualiza o status e usa o relógio do SQLite (UTC) para o tempo de término
                    cursor.execute('''
                        UPDATE requests 
                        SET status = ?, finish_request_time = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (final_status, request_id))
                    
                    conn.commit()
                    
                    if cursor.rowcount > 0:
                        self.logger.info(f"Requisição {request_id} finalizada com status '{final_status}'.")
                        return True
                    return False
                
        except Exception as e:
            self.logger.error(f"Erro ao finalizar a requisição {request_id}: {e}")
            return False


    def update_request_evaluation_data(self, request_id, provider_id, behavior, consistency, reliability, 
                                       reputation_provider, old_cred, new_cred, reputation_evaluator):
        """
        Atualiza a linha de requisição na tabela 'requests' com as métricas geradas 
        após o consumo e avaliação do serviço.
        """
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        UPDATE requests 
                        SET behavior = ?, consistency = ?, reliability = ?, 
                            reputation_provider = ?, old_credibility_evaluator = ?, 
                            new_credibility_evaluator = ?, reputation_evaluator = ?, id_provider = ?
                        WHERE id = ?
                    ''', (behavior, consistency, reliability, reputation_provider, 
                        old_cred, new_cred, reputation_evaluator, provider_id, request_id))
                    conn.commit()
                    
                    if cursor.rowcount > 0:
                        self.logger.info(f"Dados de avaliação gravados com sucesso para a requisição {request_id}.")
        except Exception as e:
            self.logger.error(f"Erro ao atualizar dados de avaliação do request {request_id}: {e}")


    # ==========================================
    # GESTÃO DE RESPOSTAS
    # ==========================================

    def save_response(self, id_request, source, ip_source, target, services_list, group_name):
        """
        Salva uma resposta recebida de um nó provedor.
        O campo services_list deve ser uma lista de tuplas/dicionários.
        """
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    cursor = conn.cursor()
                    
                    # Converte a lista de serviços para JSON para persistência
                    services_json = json.dumps(services_list)
                    
                    cursor.execute('''
                        INSERT INTO responses (id_request, source, ip_source, target, services, group_name)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (id_request, source, ip_source, target, services_json, group_name))
                    
                    conn.commit()
                    return cursor.lastrowid
        except Exception as e:
            self.logger.error(f"Erro ao salvar resposta do nó {source}: {e}")
            return None

    def get_responses_for_request(self, id_request):
        """
        Retorna todas as respostas recebidas para um ID de requisição específico.
        """
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    import sqlite3
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    
                    cursor.execute('SELECT * FROM responses WHERE id_request = ?', (id_request,))
                    rows = cursor.fetchall()
                    
                    results = []
                    for row in rows:
                        item = dict(row)
                        # Converte o JSON de volta para lista Python
                        item['services'] = json.loads(item['services'])
                        results.append(item)
                        
                    return results
        except Exception as e:
            self.logger.error(f"Erro ao buscar respostas para o pedido {id_request}: {e}")
            return []



    
    # ==========================================
    # GESTÃO DE EXPORTAÇÃO COMPLETA (API)
    # ==========================================

    def get_all_properties(self):
        """
        Retorna todas as propriedades do gateway como um dicionário.
        """

        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    # Transforma o retorno padrão em Dicionário
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    
                    cursor.execute('SELECT * FROM gateway_properties WHERE key = ?', (self._STATE_KEY,))
                    row = cursor.fetchone()
                    
                    # Converte o objeto Row nativo para um dicionário padrão do Python
                    return dict(row) if row else {}
                    
        except Exception as e:
            self.logger.error(f"Erro ao ler todas as propriedades: {e}")
            return {}

    def get_all_requests(self):
        """
        Retorna todos os registos de requisições como uma lista de dicionários.
        """

        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    
                    # Coleta todo o histórico, ordenado cronologicamente pelo ID
                    cursor.execute('SELECT * FROM requests ORDER BY id ASC')
                    rows = cursor.fetchall()
                    
                    # Cria uma lista de dicionários com todos os dados
                    return [dict(row) for row in rows]
                    
        except Exception as e:
            self.logger.error(f"Erro ao ler todas as requisições: {e}")
            return []
        
    def get_all_responses(self):
        """
        Retorna todos os registos de respostas como uma lista de dicionários.
        Faz o parse automático da string JSON da coluna 'services'.
        """
        try:
            with closing(self._get_connection()) as conn:
                with conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    
                    # Coleta todo o histórico de respostas
                    cursor.execute('SELECT * FROM responses ORDER BY id ASC')
                    rows = cursor.fetchall()
                    
                    responses_list = []
                    for row in rows:
                        row_dict = dict(row)
                        
                        # Desserializa a JSON String para recuperar a estrutura de dados original
                        services_str = row_dict.get('services')
                        if services_str:
                            try:
                                row_dict['services'] = json.loads(services_str)
                            except json.JSONDecodeError:
                                # Mantém como string caso haja uma anomalia na gravação
                                pass 
                                
                        responses_list.append(row_dict)
                        
                    return responses_list
                
        except Exception as e:
            self.logger.error(f"Erro ao ler todas as respostas: {e}")
            return []