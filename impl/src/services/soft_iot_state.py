# -*- coding: utf-8 -*-
"""
soft_iot_state.py

Estado local do gateway em SQLite (WAL): reputação própria, flag do nó
perturbador, requisições (auditoria + máquina de estados) e respostas recebidas.

Use `get_state()` para obter a instância compartilhada.
Serviço Zato: soft-iot.gateway.state.get (depuração).
"""

import json
import logging
import sqlite3
import threading
from contextlib import contextmanager

from zato.server.service import Service

from soft_iot_common import GATEWAY_STATE_DB, RequestStatus


class GatewayStateManager:

    _STATE_KEY = 'gateway_state'

    def __init__(self, db_path=GATEWAY_STATE_DB):
        self.db_path = db_path
        self.logger = logging.getLogger('zato.gateway.state')
        self._init_db()

    # ------------------------------------------------------------------
    # Infra
    # ------------------------------------------------------------------
    @contextmanager
    def _conn(self):
        """Conexão com commit/rollback automáticos. timeout evita falha por lock."""
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute('pragma journal_mode=wal')
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        try:
            with self._conn() as conn:
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS gateway_properties (
                        key TEXT PRIMARY KEY,
                        reputation TEXT,
                        behavior_changed TEXT
                    )
                ''')
                conn.execute('''
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
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS responses (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        id_request INTEGER,
                        source TEXT,
                        ip_source TEXT,
                        target TEXT,
                        services TEXT,
                        group_name TEXT,
                        FOREIGN KEY (id_request) REFERENCES requests (id)
                    )
                ''')
                conn.execute('''
                    INSERT OR IGNORE INTO gateway_properties (key, reputation, behavior_changed)
                    VALUES (?, '0.5', 'False')
                ''', (self._STATE_KEY,))
        except Exception as e:
            self.logger.error(f"Erro crítico ao inicializar a base de estado: {e}")

    def _get_property(self, column):
        with self._conn() as conn:
            row = conn.execute(
                f'SELECT {column} FROM gateway_properties WHERE key = ?', (self._STATE_KEY,)
            ).fetchone()
            return row[0] if row and row[0] is not None else None

    def _set_property(self, column, value):
        with self._conn() as conn:
            conn.execute(f'''
                INSERT INTO gateway_properties (key, {column}) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET {column} = excluded.{column}
            ''', (self._STATE_KEY, value))

    # ------------------------------------------------------------------
    # Reputação própria
    # ------------------------------------------------------------------
    def set_reputation(self, reputation_value):
        try:
            self._set_property('reputation', str(float(reputation_value)))
        except Exception as e:
            self.logger.error(f"Erro ao salvar reputação: {e}")

    def get_reputation(self):
        try:
            value = self._get_property('reputation')
            return float(value) if value is not None else 0.5
        except Exception as e:
            self.logger.error(f"Erro ao ler reputação: {e}")
            return 0.5

    # ------------------------------------------------------------------
    # Flag de mudança de conduta (nó perturbador)
    # ------------------------------------------------------------------
    def set_behavior_changed(self, changed_status):
        try:
            self._set_property('behavior_changed', str(bool(changed_status)))
        except Exception as e:
            self.logger.error(f"Erro ao salvar behavior_changed: {e}")

    def get_behavior_changed(self):
        try:
            return self._get_property('behavior_changed') == 'True'
        except Exception as e:
            self.logger.error(f"Erro ao ler behavior_changed: {e}")
            return False

    # ------------------------------------------------------------------
    # Requisições
    # ------------------------------------------------------------------
    def create_request(self, id_evaluator):
        """Cria a requisição em WAITING_RESPONSES e retorna o id (ou None)."""
        try:
            with self._conn() as conn:
                cur = conn.execute('''
                    INSERT INTO requests (id_evaluator, status, start_request_time)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                ''', (id_evaluator, RequestStatus.WAITING_RESPONSES))
                self.logger.info(f"Nova requisição criada. ID: {cur.lastrowid}")
                return cur.lastrowid
        except Exception as e:
            self.logger.error(f"Erro ao criar requisição: {e}")
            return None

    def get_last_request_status(self):
        """Retorna (id, status) da última requisição, ou (None, None)."""
        try:
            with self._conn() as conn:
                row = conn.execute('SELECT id, status FROM requests ORDER BY id DESC LIMIT 1').fetchone()
                return (row[0], row[1]) if row else (None, None)
        except Exception as e:
            self.logger.error(f"Erro ao buscar a última requisição: {e}")
            return None, None

    def get_start_request_time(self, request_id):
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT start_request_time FROM requests WHERE id = ?', (request_id,)
                ).fetchone()
                return row[0] if row and row[0] is not None else None
        except Exception as e:
            self.logger.error(f"Erro ao buscar início da requisição {request_id}: {e}")
            return None

    def get_request_status(self, request_id):
        try:
            with self._conn() as conn:
                row = conn.execute('SELECT status FROM requests WHERE id = ?', (request_id,)).fetchone()
                return row[0] if row else None
        except Exception as e:
            self.logger.error(f"Erro ao buscar status da requisição {request_id}: {e}")
            return None

    def update_request_status(self, request_id, new_status):
        try:
            with self._conn() as conn:
                cur = conn.execute('UPDATE requests SET status = ? WHERE id = ?', (new_status, request_id))
                if cur.rowcount > 0:
                    self.logger.info(f"Requisição {request_id} -> '{new_status}'.")
                    return True
                self.logger.warning(f"Atualização de status falhou: ID {request_id} não encontrado.")
                return False
        except Exception as e:
            self.logger.error(f"Erro ao atualizar status da requisição {request_id}: {e}")
            return False

    def finalize_request(self, request_id, final_status):
        try:
            with self._conn() as conn:
                cur = conn.execute('''
                    UPDATE requests SET status = ?, finish_request_time = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (final_status, request_id))
                if cur.rowcount > 0:
                    self.logger.info(f"Requisição {request_id} finalizada: '{final_status}'.")
                    return True
                return False
        except Exception as e:
            self.logger.error(f"Erro ao finalizar a requisição {request_id}: {e}")
            return False

    def update_request_evaluation_data(self, request_id, provider_id, behavior, consistency, reliability,
                                       reputation_provider, old_cred, new_cred, reputation_evaluator):
        try:
            with self._conn() as conn:
                conn.execute('''
                    UPDATE requests
                    SET behavior = ?, consistency = ?, reliability = ?,
                        reputation_provider = ?, old_credibility_evaluator = ?,
                        new_credibility_evaluator = ?, reputation_evaluator = ?, id_provider = ?
                    WHERE id = ?
                ''', (behavior, consistency, reliability, reputation_provider,
                      old_cred, new_cred, reputation_evaluator, provider_id, request_id))
        except Exception as e:
            self.logger.error(f"Erro ao gravar dados de avaliação da requisição {request_id}: {e}")

    # ------------------------------------------------------------------
    # Respostas (propostas dos provedores)
    # ------------------------------------------------------------------
    def save_response(self, id_request, source, ip_source, target, services_list, group_name):
        try:
            with self._conn() as conn:
                cur = conn.execute('''
                    INSERT INTO responses (id_request, source, ip_source, target, services, group_name)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (id_request, source, ip_source, target, json.dumps(services_list), group_name))
                return cur.lastrowid
        except Exception as e:
            self.logger.error(f"Erro ao salvar resposta do nó {source}: {e}")
            return None

    @staticmethod
    def _response_row_to_dict(row):
        item = dict(row)
        try:
            item['services'] = json.loads(item['services']) if item.get('services') else []
        except (ValueError, TypeError):
            pass
        return item

    def get_responses_for_request(self, id_request):
        try:
            with self._conn() as conn:
                rows = conn.execute('SELECT * FROM responses WHERE id_request = ?', (id_request,)).fetchall()
                return [self._response_row_to_dict(r) for r in rows]
        except Exception as e:
            self.logger.error(f"Erro ao buscar respostas do pedido {id_request}: {e}")
            return []

    # ------------------------------------------------------------------
    # Exportação completa (usada pelo serviço de debug)
    # ------------------------------------------------------------------
    def get_all_properties(self):
        try:
            with self._conn() as conn:
                row = conn.execute(
                    'SELECT * FROM gateway_properties WHERE key = ?', (self._STATE_KEY,)
                ).fetchone()
                return dict(row) if row else {}
        except Exception as e:
            self.logger.error(f"Erro ao ler propriedades: {e}")
            return {}

    def get_all_requests(self):
        try:
            with self._conn() as conn:
                return [dict(r) for r in conn.execute('SELECT * FROM requests ORDER BY id ASC').fetchall()]
        except Exception as e:
            self.logger.error(f"Erro ao ler requisições: {e}")
            return []

    def get_all_responses(self):
        try:
            with self._conn() as conn:
                rows = conn.execute('SELECT * FROM responses ORDER BY id ASC').fetchall()
                return [self._response_row_to_dict(r) for r in rows]
        except Exception as e:
            self.logger.error(f"Erro ao ler respostas: {e}")
            return []


# ----------------------------------------------------------------------
# Instância compartilhada
# ----------------------------------------------------------------------
_state = None
_state_lock = threading.Lock()


def get_state():
    global _state
    if _state is None:
        with _state_lock:
            if _state is None:
                _state = GatewayStateManager()
    return _state


# ----------------------------------------------------------------------
# Serviço Zato (debug)
# ----------------------------------------------------------------------
class GetGatewayStateService(Service):
    """Exporta todo o estado local do gateway (propriedades, requisições, respostas)."""
    name = 'soft-iot.gateway.state.get'

    def handle(self):
        try:
            gs = get_state()
            properties = gs.get_all_properties()
            requests_data = gs.get_all_requests()
            responses_data = gs.get_all_responses()

            self.response.payload = {
                "status": "success",
                "data": {
                    "gateway_properties": properties,
                    "requests_history": requests_data,
                    "responses_history": responses_data,
                    "metrics": {
                        "total_requests_logged": len(requests_data),
                        "total_responses_logged": len(responses_data),
                    },
                },
            }
        except Exception as e:
            self.logger.error(f"Erro ao recuperar o estado do gateway: {e}")
            self.response.payload = {"status": "error", "message": "Falha interna ao ler a base local."}