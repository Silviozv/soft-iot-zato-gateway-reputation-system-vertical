# -*- coding: utf-8 -*-

import os
import json
import math
import time

from zato.server.service import Service


# Faixa de valores válidos gerados pelos sensores virtuais.
# O device envia -1.0 quando decide mandar um dado inválido.
VALID_MIN = 0.0
VALID_MAX = 100.0

# Valor injetado pelo gateway provedor malicioso
INVALID_DATA_VALUE = "-1.0"

# Índices na Tangle
DEVICES_INDEX_PREFIX = "DEVS_"       # DEVS_<gateway_id>: devices registrados pelo gateway
DEVICE_EVAL_INDEX_PREFIX = "DEV_"    # DEV_<device_id>: avaliações do device feitas pelo seu gateway


def is_valid_reading(value):
    """
    Retorna True se o dado de sensoriamento for válido (numérico e dentro da faixa).
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False

    return math.isfinite(number) and VALID_MIN <= number <= VALID_MAX


def _parse_tx_data(tx):
    """
    Extrai o campo 'data' de uma mensagem lida da Tangle (pode vir como string JSON).
    """
    if not isinstance(tx, dict):
        return None

    content = tx.get('data')

    if isinstance(content, str):
        try:
            content = json.loads(content)
        except json.JSONDecodeError:
            return None

    return content if isinstance(content, dict) else None


class DeviceRegisterService(Service):
    """
    Registra na Tangle um device que iniciou a comunicação com este gateway.
    Chamado pelo controlador de armazenamento local no handshake (dev/CONNECTIONS).
    """
    name = 'soft-iot.device.register'

    def handle(self):
        payload = self.request.payload or {}
        device_id = payload.get('device_id')
        device = payload.get('device') or {}

        if not device_id:
            self.logger.error("Registro de device sem device_id.")
            self.response.payload = {"status": "error", "message": "Missing device_id"}
            return

        identity_info = self.invoke('soft-iot.id.manager')
        my_gateway_id = identity_info.get('gateway_id')
        my_group = identity_info.get('tangle_group')

        if not my_gateway_id:
            self.response.payload = {"status": "error", "message": "no_id"}
            return

        # Mantém apenas id e tipo dos sensores (o que importa para a busca por serviço)
        sensors = []
        for sensor in device.get('sensors', []):
            sensors.append({
                "id": str(sensor.get('id')).strip(),
                "type": sensor.get('type')
            })

        register_transaction = {
            "type": "DEVICE_REGISTER",
            "gateway_id": my_gateway_id,
            "group": my_group,
            "device_id": device_id,
            "latitude": device.get('latitude'),
            "longitude": device.get('longitude'),
            "sensors": sensors,
            "timestamp": int(time.time())
        }

        res = self.invoke('soft-iot.dlt.client.api.write', {
            "index": f"{DEVICES_INDEX_PREFIX}{my_gateway_id}",
            "data": register_transaction
        })

        if res.get('status') == 'success':
            self.logger.info(f"[REGISTRO DEVICE] Device {device_id} registrado na Tangle ({len(sensors)} sensores).")
        else:
            self.logger.warning(f"[REGISTRO DEVICE] Falha ao registrar {device_id} na Tangle: {res}")

        self.response.payload = {"status": res.get('status', 'error'), "device_id": device_id}


class DeviceEvaluationService(Service):
    """
    Avalia os dados recebidos de um device conectado (fluxo local) de acordo
    com a conduta de avaliador do gateway e grava as notas na Tangle.
    Cada leitura recebe nota 0 ou 1. A credibilidade do gateway não é usada.
    """
    name = 'soft-iot.device.evaluation'

    def handle(self):
        payload = self.request.payload or {}
        device_id = payload.get('device_id')
        values = payload.get('values') or []

        # Lote acumulado pelo controlador (vários sensores) ou chamada com um sensor só
        sensor_ids = payload.get('sensor_ids')
        if not sensor_ids and payload.get('sensor_id'):
            sensor_ids = [payload.get('sensor_id')]
        sensor_ids = sensor_ids or []

        if not device_id or not values:
            self.response.payload = {"status": "ignored", "message": "Missing device_id or values"}
            return

        # Import local: o Zato carrega os arquivos em ordem alfabética, e este módulo
        # vem antes dos que ele usa. No momento da chamada, todos já estão carregados.
        from soft_iot_node_type import NodeTypeManager
        from soft_iot_gateway_state import GatewayStateManager

        nt_manager = NodeTypeManager()
        gs_manager = GatewayStateManager()
        behavior_changed = gs_manager.get_behavior_changed()

        evaluations = []
        conducts = []

        for value in values:
            raw_evaluation = 1 if is_valid_reading(value) else 0

            # A conduta é sorteada para cada dado recebido
            nt_manager.define_conduct(behavior_changed)
            conduct = nt_manager.current_conduct

            if conduct == "SELFISH":
                # Egoísta consome o dado mas não avalia
                self.logger.info(f"[AVALIAÇÃO DEVICE] Nó egoísta não avalia o device {device_id}.")
                self.response.payload = {"status": "ignored", "conduct_applied": conduct}
                return

            if conduct == "MALICIOUS":
                final_evaluation = 1 - raw_evaluation
            else:
                final_evaluation = raw_evaluation

            evaluations.append(final_evaluation)
            conducts.append(conduct)

        identity_info = self.invoke('soft-iot.id.manager')
        my_gateway_id = identity_info.get('gateway_id')
        my_group = identity_info.get('tangle_group')

        evaluation_transaction = {
            "type": "REP_DEVICE_EVALUATION",
            "source": my_gateway_id,
            "group": my_group,
            "target": device_id,
            "sensor_ids": sensor_ids,
            "serviceEvaluations": evaluations,
            "timestamp": int(time.time())
        }

        res = self.invoke('soft-iot.dlt.client.api.write', {
            "index": f"{DEVICE_EVAL_INDEX_PREFIX}{device_id}",
            "data": evaluation_transaction
        })

        self.logger.info(
            f"[AVALIAÇÃO DEVICE] {device_id} {sensor_ids} | valores: {values} | "
            f"condutas: {conducts} | notas: {evaluations} | escrita: {res.get('status')}"
        )

        self.response.payload = {"status": res.get('status', 'error'), "evaluations": evaluations}


class GatewayDevicesLookupService(Service):
    """
    Consulta na Tangle os devices registrados por um gateway e retorna
    os pares (device_id, sensor_id) que oferecem o serviço pedido.
    """
    name = 'soft-iot.dlt.gateway.devices'

    def handle(self):
        payload = self.request.payload or {}
        gateway_id = payload.get('gateway_id')
        requested_service = payload.get('service')

        if not gateway_id:
            self.response.payload = []
            return

        tangle_res = self.invoke('soft-iot.dlt.client.api.read_index', {
            'index': f"{DEVICES_INDEX_PREFIX}{gateway_id}"
        })

        registers = []
        if isinstance(tangle_res, list):
            for tx in tangle_res:
                content = _parse_tx_data(tx)
                if content and content.get('type') == 'DEVICE_REGISTER' and content.get('gateway_id') == gateway_id:
                    registers.append(content)

        # O registro mais recente de cada device prevalece
        registers.sort(key=lambda x: int(x.get('timestamp', 0)), reverse=True)

        seen_devices = set()
        matched = []

        for register in registers:
            device_id = register.get('device_id')
            if not device_id or device_id in seen_devices:
                continue
            seen_devices.add(device_id)

            for sensor in register.get('sensors', []):
                if requested_service is None or sensor.get('type') == requested_service:
                    matched.append({
                        "device_id": device_id,
                        "sensor_id": sensor.get('id')
                    })
                    break

        self.logger.info(f"Devices do gateway {gateway_id} com o serviço '{requested_service}': {matched}")

        self.response.payload = matched


class DeviceReputationService(Service):
    """
    Calcula a reputação de um device a partir das notas (0 ou 1) gravadas pelo seu gateway.
    Usa o mesmo prior neutro (0.5) da reputação dos gateways.
    """
    name = 'soft-iot.reputation.device'

    def handle(self):
        payload = self.request.payload or {}
        device_id = payload.get('device_id')
        gateway_id = payload.get('gateway_id')  # Opcional: aceita apenas notas do gateway dono

        if not device_id:
            self.response.payload = {"status": "error", "message": "device_id é obrigatório"}
            return

        tangle_res = self.invoke('soft-iot.dlt.client.api.read_index', {
            'index': f"{DEVICE_EVAL_INDEX_PREFIX}{device_id}"
        })

        evaluations = []
        if isinstance(tangle_res, list):
            for tx in tangle_res:
                content = _parse_tx_data(tx)
                if not content or content.get('type') != 'REP_DEVICE_EVALUATION':
                    continue
                if content.get('target') != device_id:
                    continue
                if gateway_id and content.get('source') != gateway_id:
                    continue

                for evaluation in content.get('serviceEvaluations', []):
                    try:
                        evaluations.append(float(evaluation))
                    except (TypeError, ValueError):
                        continue

        if not evaluations:
            self.logger.warning(f"Sem avaliações na Tangle para o device {device_id}. Retornando 0.5 (neutro).")
            self.response.payload = {"device_id": device_id, "reputation": 0.5, "status": "no_data"}
            return

        PRIOR = 0.5
        PRIOR_WEIGHT = float(os.environ.get('Zato_REP_PRIOR_WEIGHT', '5'))

        reputation = (sum(evaluations) + PRIOR * PRIOR_WEIGHT) / (len(evaluations) + PRIOR_WEIGHT)
        reputation = max(0.0, min(1.0, reputation))

        self.logger.info(
            f"[REPUTAÇÃO DEVICE] Device: {device_id} | Reputação: {reputation:.4f} | "
            f"Avaliações: {len(evaluations)}"
        )

        self.response.payload = {
            "device_id": device_id,
            "reputation": round(reputation, 4),
            "evaluations_used": len(evaluations),
            "status": "success"
        }
