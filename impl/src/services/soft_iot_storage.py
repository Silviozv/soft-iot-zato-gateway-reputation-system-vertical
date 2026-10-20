# -*- coding: utf-8 -*-
"""
soft_iot_storage.py

Ciclo de vida dos dados locais dos sensores:
  - LocalStorageController: broker MQTT (TATU), handshake, auto-cadastro e persistência
  - manutenção: agregação em janelas + limpeza (um único serviço, em ordem)
  - API REST de leitura consumida por outros gateways

Serviços Zato:
  soft-iot.storage.maintenance   (agregação seguida de limpeza)
  soft-iot.api.get-last-data     (último dado de um sensor)

A inicialização do controller é feita por soft_iot_bootstrap.py.
"""

import json
import logging
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt

from zato.server.service import Service

from soft_iot_common import (
    DB_FILENAME, env_int, find_device, load_devices, save_devices,
)

try:
    from extended_tatu_wrapper.utils import tatu_wrapper
except ImportError:
    logging.error("CRITICAL: extended_tatu_wrapper not found. Please install it via pip.")
    tatu_wrapper = None

BROKER_URL = "0.0.0.0"
BROKER_PORT_TCP = 1883
DEFAULT_COLLECTION_TIME = env_int('Zato_COLLECTION_TIME', 5)
DEFAULT_PUBLISHING_TIME = env_int('Zato_PUBLISH_TIME', 15)
DEFAULT_RETENTION_SECONDS = 60


# ===========================================================================
# Controller MQTT + SQLite
# ===========================================================================
class LocalStorageController:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    inst = super(LocalStorageController, cls).__new__(cls)
                    inst.is_running = False
                    inst.client = None
                    inst.pending_registration = {}
                    cls._instance = inst
        return cls._instance

    def start(self, logger):
        if self.is_running:
            return
        self.logger = logger
        self.logger.info("Iniciando Soft-IoT Local Storage Controller...")
        self.pending_registration = {}
        self.init_db()
        self.connect_mqtt()
        self.is_running = True

    # -- banco ---------------------------------------------------------
    def init_db(self):
        try:
            with sqlite3.connect(DB_FILENAME) as conn:
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS sensor_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sensor_id TEXT,
                        device_id TEXT,
                        data_value TEXT,
                        start_datetime TIMESTAMP,
                        end_datetime TIMESTAMP,
                        aggregation_status INTEGER DEFAULT 0
                    )
                ''')
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS aggregation_registered_last_time_sensors (
                        sensor_id TEXT,
                        device_id TEXT,
                        last_time TIMESTAMP
                    )
                ''')
                conn.commit()
            self.logger.info(f"Banco SQLite inicializado em {DB_FILENAME}")
        except Exception as e:
            self.logger.error(f"Erro ao inicializar banco de dados: {e}")

    # -- MQTT ----------------------------------------------------------
    def connect_mqtt(self):
        client_id = f"SoftIoT_Storage_{int(time.time())}"
        self.client = mqtt.Client(client_id=client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        try:
            self.logger.info(f"Conectando ao broker MQTT {BROKER_URL}:{BROKER_PORT_TCP}...")
            self.client.connect(BROKER_URL, BROKER_PORT_TCP, 60)
            self.client.loop_start()
        except Exception as e:
            self.logger.error(f"Falha ao conectar no MQTT: {e}")

    def on_connect(self, client, userdata, flags, rc):
        self.logger.info(f"MQTT conectado com código: {rc}")
        topics = [("CONNECTED", 1), ("dev/CONNECTIONS", 1), ("dev/#", 1)]
        client.subscribe(topics)
        self.logger.info(f"Inscrito nos tópicos: {topics}")

    def on_disconnect(self, client, userdata, rc):
        self.logger.warning("MQTT desconectado. O Paho tentará reconectar (loop_start).")

    def on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8').strip()

            if not payload or tatu_wrapper is None:
                return
            if topic == "dev/CONNECTIONS/RES":
                return
            if topic == "dev/CONNECTIONS":
                self.handle_connect_request(payload)
                return
            if tatu_wrapper.is_tatu_response(payload):
                self.handle_tatu_response(payload)
        except Exception as e:
            self.logger.error(f"Erro no processamento MQTT: {e}")

    # -- handshake e auto-cadastro --------------------------------------
    def handle_connect_request(self, payload):
        """Processa o CONNECT ('CONNECT VALUE BROKER {...}') e responde CONNACK."""
        if not payload:
            return
        try:
            json_start = payload.find('{')
            if json_start == -1:
                self.logger.error("Payload CONNECT inválido: nenhum objeto JSON encontrado.")
                return

            data = json.loads(payload[json_start:])
            device_id = data.get("HEADER", {}).get("NAME")
            device_structure = data.get("DEVICE")

            if not device_id:
                self.logger.warning("CONNECT recebido sem nome do dispositivo.")
                return

            if device_structure:
                self.pending_registration[device_id] = device_structure

            self.logger.info(f"CONNECT de {device_id}. Enviando CONNACK...")
            connack = {
                "CODE": "POST",
                "METHOD": "CONNACK",
                "HEADER": {"NAME": "Zato-Gateway", "TIMESTAMP": int(time.time() * 1000)},
                "BODY": {"NEW_NAME": device_id, "CAN_CONNECT": True},
            }
            self.client.publish("dev/CONNECTIONS/RES", json.dumps(connack))

            time.sleep(0.5)
            self.handle_device_connected(device_id)
        except Exception as e:
            self.logger.error(f"Erro ao processar CONNECT: {e}")

    def handle_device_connected(self, device_id):
        """Auto-cadastra o dispositivo (se preciso) e envia FLOW para cada sensor."""
        self.logger.info(f"Dispositivo conectado: {device_id}")
        try:
            device_info = find_device(device_id)

            if not device_info:
                self.logger.warning(f"Dispositivo {device_id} não encontrado. Verificando auto-cadastro...")
                if device_id not in self.pending_registration:
                    self.logger.error(f"Sem dados para auto-cadastrar {device_id}.")
                    return

                device_info = self.pending_registration.pop(device_id)
                device_info['id'] = device_id
                for sensor in device_info.get('sensors', []):
                    sensor['collection_time'] = DEFAULT_COLLECTION_TIME
                    sensor['publishing_time'] = DEFAULT_PUBLISHING_TIME

                devices = load_devices()
                devices.append(device_info)
                save_devices(devices)
                self.logger.info(f"Dispositivo {device_id} auto-cadastrado.")

            sensors = device_info.get('sensors', [])
            self.logger.info(f"Configurando {len(sensors)} sensores de {device_id}...")

            for sensor in sensors:
                sensor_id = str(sensor.get('id')).strip()
                c_time = int(sensor.get('collection_time', DEFAULT_COLLECTION_TIME) * 1000)
                p_time = int(sensor.get('publishing_time', DEFAULT_PUBLISHING_TIME) * 1000)

                flow_body = json.dumps({"collect": c_time, "publish": p_time}, separators=(',', ':'))
                flow_req = f"FLOW VALUE {sensor_id} {flow_body}"

                self.client.publish(f"dev/{device_id}", flow_req)
                time.sleep(0.2)

            self.logger.info(f"Comandos FLOW enviados para {device_id}")
        except Exception as e:
            self.logger.error(f"Erro ao configurar dispositivo conectado: {e}")

    # -- dados ----------------------------------------------------------
    def handle_tatu_response(self, message_content):
        try:
            device_id = tatu_wrapper.get_device_id_by_tatu_answer(message_content)
            sensor_id = tatu_wrapper.get_sensor_id_by_tatu_answer(message_content)
            data_list = self.parse_tatu_body_to_data(message_content)

            if data_list:
                self.logger.info(f"Armazenando {len(data_list)} registros de {device_id}/{sensor_id}")
                self.store_sensor_data(device_id, sensor_id, data_list)
            else:
                self.logger.warning("Resposta TATU válida, mas sem dados extraíveis.")
        except Exception as e:
            self.logger.error(f"Erro ao processar resposta TATU: {e}")

    def parse_tatu_body_to_data(self, json_str):
        """Converte o BODY de uma resposta TATU (GET ou FLOW) em lista de {value, timestamp}."""
        data_points = []
        try:
            obj = json.loads(json_str)
            body = obj.get("BODY", {})
            method = obj.get("METHOD")
            header_ts = int(obj.get("HEADER", {}).get("TIMESTAMP", time.time() * 1000))

            for key, value in body.items():
                if key == "FLOW":
                    continue
                values = value if isinstance(value, list) else [value]

                if method == "GET":
                    data_points.append({
                        "value": str(values[0]),
                        "timestamp": datetime.fromtimestamp(header_ts / 1000.0),
                    })
                else:
                    flow_info = body.get("FLOW", {})
                    collect_ms = flow_info.get("collect", DEFAULT_COLLECTION_TIME * 1000)
                    if collect_ms < 100:
                        collect_ms = DEFAULT_COLLECTION_TIME * 1000

                    current_ts = header_ts
                    for v in reversed(values):
                        data_points.append({
                            "value": str(v),
                            "timestamp": datetime.fromtimestamp(current_ts / 1000.0),
                        })
                        current_ts -= collect_ms
                    data_points.reverse()
        except Exception as e:
            self.logger.error(f"Erro no parsing JSON do TATU: {e}")
        return data_points

    def store_sensor_data(self, device_id, sensor_id, data_list):
        # Zato_SAVE_DATA_ENABLED=False desliga a persistência (padrão: ligada)
        if os.getenv('Zato_SAVE_DATA_ENABLED', 'True') == 'False':
            self.logger.info(f"[PERSISTÊNCIA OFF] Ignorando {len(data_list)} registros de {device_id}/{sensor_id}")
            return
        try:
            batch = [
                (sensor_id, device_id, str(item['value']), item['timestamp'], item['timestamp'])
                for item in data_list
            ]
            if not batch:
                return
            with sqlite3.connect(DB_FILENAME) as conn:
                conn.executemany('''
                    INSERT INTO sensor_data (sensor_id, device_id, data_value, start_datetime, end_datetime, aggregation_status)
                    VALUES (?, ?, ?, ?, ?, 0)
                ''', batch)
                conn.commit()
            self.logger.info(f"Salvos {len(batch)} registros de {device_id}/{sensor_id}")
        except Exception as e:
            self.logger.error(f"Erro SQL ao inserir dados: {e}")


# ===========================================================================
# Manutenção: agregação + limpeza
# ===========================================================================
def _open_maintenance_db():
    conn = sqlite3.connect(DB_FILENAME)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.row_factory = sqlite3.Row
    return conn


def _aggregate_sensor(cursor, device_id, sensor_id, window_seconds, logger):
    cursor.execute("""
        SELECT last_time FROM aggregation_registered_last_time_sensors
        WHERE device_id = ? AND sensor_id = ?
    """, (device_id, sensor_id))
    last_run_row = cursor.fetchone()

    if last_run_row and last_run_row[0]:
        last_time = last_run_row[0]
    else:
        last_time = "1970-01-01 00:00:00"
        if not last_run_row:
            cursor.execute("""
                INSERT INTO aggregation_registered_last_time_sensors (device_id, sensor_id, last_time)
                VALUES (?, ?, ?)
            """, (device_id, sensor_id, last_time))

    # Só considera blocos já fechados (anteriores ao bloco atual do relógio)
    query = """
        SELECT
            datetime((CAST(strftime('%s', start_datetime) AS INTEGER) / ?) * ?, 'unixepoch') as bucket_start,
            AVG(CAST(data_value AS FLOAT)) as avg_val,
            MAX(end_datetime) as last_entry_in_bucket
        FROM sensor_data
        WHERE device_id = ?
          AND sensor_id = ?
          AND aggregation_status = 0
          AND start_datetime > ?
          AND start_datetime < datetime((CAST(strftime('%s', 'now') AS INTEGER) / ?) * ?, 'unixepoch')
        GROUP BY bucket_start
        ORDER BY bucket_start ASC
    """
    cursor.execute(query, (window_seconds, window_seconds, device_id, sensor_id,
                           last_time, window_seconds, window_seconds))
    aggregates = cursor.fetchall()
    if not aggregates:
        return

    logger.info(f"Agregando {len(aggregates)} blocos de {window_seconds // 60} min para {device_id}/{sensor_id}")

    last_processed_time = last_time
    for agg in aggregates:
        bucket_start = agg['bucket_start']
        avg_value = round(agg['avg_val'], 2)

        cursor.execute("""
            INSERT INTO sensor_data
            (sensor_id, device_id, data_value, start_datetime, end_datetime, aggregation_status)
            VALUES (?, ?, ?, ?, ?, 1)
        """, (sensor_id, device_id, str(avg_value), bucket_start, bucket_start))

        # status 2 = bruto já agregado (elegível para limpeza)
        cursor.execute("""
            UPDATE sensor_data
            SET aggregation_status = 2
            WHERE device_id = ? AND sensor_id = ? AND aggregation_status = 0
              AND start_datetime >= ?
              AND start_datetime < datetime(?, '+' || ? || ' seconds')
        """, (device_id, sensor_id, bucket_start, bucket_start, window_seconds))
        last_processed_time = agg['last_entry_in_bucket']

    cursor.execute("""
        UPDATE aggregation_registered_last_time_sensors
        SET last_time = ? WHERE device_id = ? AND sensor_id = ?
    """, (last_processed_time, device_id, sensor_id))


def run_aggregation(logger):
    window_minutes = env_int('Zato_AGGREGATION_WINDOW_MINUTES', 60)
    window_seconds = window_minutes * 60
    logger.info(f"INICIANDO AGREGAÇÃO (janela: {window_minutes} min)...")

    if not os.path.exists(DB_FILENAME):
        return

    conn = None
    try:
        conn = _open_maintenance_db()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT device_id, sensor_id FROM sensor_data")
        for row in cursor.fetchall():
            _aggregate_sensor(cursor, row['device_id'], row['sensor_id'], window_seconds, logger)
        conn.commit()
        logger.info("AGREGAÇÃO FINALIZADA.")
    except Exception as e:
        logger.error(f"Erro na agregação: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def run_cleanup(logger):
    retention_seconds = env_int('Zato_DATA_RETENTION_SECONDS', DEFAULT_RETENTION_SECONDS)
    cutoff = (datetime.now() - timedelta(seconds=retention_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"INICIANDO LIMPEZA: removendo registros agregados anteriores a {cutoff} ({retention_seconds}s)")

    if not os.path.exists(DB_FILENAME):
        logger.warning("Banco de dados não encontrado. Limpeza ignorada.")
        return

    conn = None
    try:
        conn = _open_maintenance_db()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM sensor_data WHERE end_datetime <= ? AND aggregation_status = 2", (cutoff,))
        deleted = cursor.rowcount
        conn.commit()
        logger.info(f"LIMPEZA CONCLUÍDA: {deleted} registros removidos.")
    except Exception as e:
        logger.error(f"Erro na limpeza de dados: {e}")
    finally:
        if conn:
            conn.close()


class StorageMaintenanceService(Service):
    """Agrega as leituras brutas e, em seguida, limpa as já agregadas e expiradas."""
    name = 'soft-iot.storage.maintenance'

    def handle(self):
        run_aggregation(self.logger)
        run_cleanup(self.logger)
        self.response.payload = {"status": "success"}


# ===========================================================================
# API REST (consumida por outros gateways)
# ===========================================================================
class GetLastData(Service):
    """Último dado de um sensor: GET /soft-iot/devices/{device_id}/sensors/{sensor_id}/data/latest"""
    name = 'soft-iot.api.get-last-data'

    def _require_device_and_sensor(self):
        params = getattr(self.request.http, 'params', None) or {}
        device_id = params.get('device_id')
        sensor_id = params.get('sensor_id')

        if not device_id or not sensor_id:
            path = (getattr(self, 'wsgi_environ', None) or {}).get('PATH_INFO', '')
            match = re.search(r'/devices/([^/]+)/sensors/([^/]+)', path)
            if match:
                device_id = device_id or match.group(1)
                sensor_id = sensor_id or match.group(2)

        return device_id, sensor_id

    def _get_last(self, device_id, sensor_id):
        if not os.path.exists(DB_FILENAME):
            raise ValueError(f"Banco de dados não encontrado em {DB_FILENAME}")

        conn = sqlite3.connect(DB_FILENAME, timeout=5000)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute('PRAGMA journal_mode=WAL;')
            row = conn.execute("""
                SELECT * FROM sensor_data
                WHERE device_id = ? AND sensor_id = ?
                ORDER BY start_datetime DESC LIMIT 1
            """, (device_id, sensor_id)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def handle(self):
        device_id, sensor_id = self._require_device_and_sensor()

        if not device_id or not sensor_id:
            self.response.status_code = 400
            self.response.payload = {'error': 'device_id and sensor_id are required'}
            return

        try:
            item = self._get_last(device_id, sensor_id)
            if item is None:
                self.logger.warning(f"Sensor {device_id}/{sensor_id} não encontrado no banco local.")
                self.response.status_code = 404
                self.response.payload = {'error': 'Device or sensor not found', 'data': None}
                return
            self.response.payload = {'data': item}
        except Exception as e:
            self.logger.error(f"Erro na API GetLastData: {e}")
            self.response.status_code = 500
            self.response.payload = {'error': str(e)}