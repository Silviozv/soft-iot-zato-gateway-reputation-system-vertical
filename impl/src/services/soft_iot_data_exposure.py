# -*- coding: utf-8 -*-

from zato.server.service import Service
import sqlite3
import os
import json
import re

# Caminho do banco de dados (mesmo usado pelo storage)
DB_FILENAME = "/opt/zato/env/soft_iot_data.db"

class BaseAPIService(Service):
    """Classe base para conexão com banco e formatação."""
    
    def _get_conn(self):
        if not os.path.exists(DB_FILENAME):
            raise ValueError(f"Banco de dados não encontrado em {DB_FILENAME}")
        
        # Adiciona timeout para evitar 'Database is locked' durante a agregação/cleanup
        conn = sqlite3.connect(DB_FILENAME, timeout=5000)
        conn.row_factory = sqlite3.Row 
        
        # Ativa o modo WAL para permitir leituras enquanto o storage escreve
        conn.execute('PRAGMA journal_mode=WAL;')
        return conn

    def _format_rows(self, cursor):
        results = []
        for row in cursor.fetchall():
            results.append(dict(row))
        return results

    def _require_device_and_sensor(self):
        device_id = None
        sensor_id = None

        # Path parameters do canal REST
        params = getattr(self.request.http, 'params', None) or {}
        device_id = params.get('device_id')
        sensor_id = params.get('sensor_id')

        # Fallback: extrair do path bruto
        if not device_id or not sensor_id:
            path = (getattr(self, 'wsgi_environ', None) or {}).get('PATH_INFO', '')
            match = re.search(r'/devices/([^/]+)/sensors/([^/]+)', path)
            if match:
                device_id = device_id or match.group(1)
                sensor_id = sensor_id or match.group(2)

        if not device_id or not sensor_id:
            self.logger.warning(
                f"Parâmetros ausentes. http.params={params} "
                f"PATH_INFO={(getattr(self, 'wsgi_environ', None) or {}).get('PATH_INFO')}"
            )
            self.response.payload = {'error': 'device_id and sensor_id are required'}
            self.response.status_code = 400
            return None, None

        return device_id, sensor_id

    def _get_last(self, device_id, sensor_id, *, aggregated=None):
        """
        aggregated:
          - None: não filtra por aggregation_status
          - True: aggregation_status = 1
          - False: aggregation_status != 1
        """
        query = """
            SELECT * FROM sensor_data
            WHERE device_id = ? AND sensor_id = ?
        """
        params = [device_id, sensor_id]

        if aggregated is True:
            query += " AND aggregation_status = 1"
        elif aggregated is False:
            query += " AND aggregation_status != 1"

        query += " ORDER BY start_datetime DESC LIMIT 1"

        conn = self._get_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            data = self._format_rows(cursor)
            return data[0] if data else None
        finally:
            conn.close()

    def _get_history(self, device_id, sensor_id, *, aggregated=None, start_date=None, end_date=None, limit=100, order="DESC"):
        query = "SELECT * FROM sensor_data WHERE device_id = ? AND sensor_id = ?"
        params = [device_id, sensor_id]

        if aggregated is True:
            query += " AND aggregation_status = 1"
        elif aggregated is False:
            query += " AND aggregation_status != 1"

        if start_date:
            query += " AND start_datetime >= ?"
            params.append(start_date)

        if end_date:
            query += " AND end_datetime <= ?"
            params.append(end_date)

        if str(order).upper() not in {"ASC", "DESC"}:
            order = "DESC"

        query += f" ORDER BY start_datetime {order} LIMIT ?"
        params.append(limit)

        conn = self._get_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return self._format_rows(cursor)
        finally:
            conn.close()

class GetLastData(BaseAPIService):
    """Último dado sem filtrar agregação."""
    name = 'soft-iot.api.get-last-data'

    def handle(self):
        device_id, sensor_id = self._require_device_and_sensor()
        
        if not device_id:
            return

        try:
            item = self._get_last(device_id, sensor_id, aggregated=None)

            if item is None:
                self.logger.warning(f"Sensor {device_id}/{sensor_id} não encontrado no banco local.")
                self.response.status_code = 404
                self.response.payload = {'error': 'Device or sensor not found', 'data': None}
                return
            
            self.response.payload = {'data': item}
            
        except Exception as e:
            self.logger.error(f"Erro na API GetLastData: {e}")
            self.response.payload = {'error': str(e)}
            self.response.status_code = 500

class GetSensorHistory(BaseAPIService):
    """
    Retorna histórico de dados com filtros opcionais de data.
    """
    name = 'soft-iot.api.get-history'

    def handle(self):
        device_id, sensor_id = self._require_device_and_sensor()
        if not device_id:
            return

        start_date = self.request.payload.get('start_date')  # Esperado formato ISO ou timestamp
        end_date = self.request.payload.get('end_date')
        limit = self.request.payload.get('limit', 100)

        try:
            data = self._get_history(
                device_id,
                sensor_id,
                aggregated=None,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                order="DESC",
            )
            self.response.payload = {'count': len(data), 'data': data}
        except Exception as e:
            self.logger.error(f"Erro na API GetSensorHistory: {e}")
            self.response.payload = {'error': str(e)}
            self.response.status_code = 500

class GetLastRawSensorData(BaseAPIService):
    """Última leitura bruta (não agregada)."""
    name = 'soft-iot.api.get-last-sensor-data'

    def handle(self):
        device_id, sensor_id = self._require_device_and_sensor()
        if not device_id:
            return

        try:
            item = self._get_last(device_id, sensor_id, aggregated=False)
            self.response.payload = {'data': item}
        except Exception as e:
            self.logger.error(f"Erro na API GetLastRawSensorData: {e}")
            self.response.payload = {'error': str(e)}
            self.response.status_code = 500

class GetLastAggregatedData(BaseAPIService):
    """
    Retorna a última média calculada para o sensor.
    """
    name = 'soft-iot.api.get-last-aggregated-data'

    def handle(self):
        device_id, sensor_id = self._require_device_and_sensor()
        if not device_id:
            return

        try:
            item = self._get_last(device_id, sensor_id, aggregated=True)
            self.response.payload = {'data': item}
        except Exception as e:
            self.logger.error(f"Erro em GetLastAggregatedData: {e}")
            self.response.payload = {'error': str(e)}
            self.response.status_code = 500

class GetAggregatedHistory(BaseAPIService):
    """Histórico agregado (lista)."""
    name = 'soft-iot.api.get-all-aggregated-data'

    def handle(self):
        device_id, sensor_id = self._require_device_and_sensor()
        if not device_id:
            return

        limit = self.request.payload.get('limit', 1000)
        start_date = self.request.payload.get('start_date')
        end_date = self.request.payload.get('end_date')

        try:
            data = self._get_history(
                device_id,
                sensor_id,
                aggregated=True,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                order="ASC",
            )
            self.response.payload = {'count': len(data), 'data': data}
        except Exception as e:
            self.logger.error(f"Erro em GetAggregatedHistory: {e}")
            self.response.payload = {'error': str(e)}
            self.response.status_code = 500
