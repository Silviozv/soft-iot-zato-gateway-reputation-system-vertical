# -*- coding: utf-8 -*-

import json
import os
from zato.server.service import Service

# Caminho para o arquivo de configuração.
# O Zato roda a partir da pasta do servidor, então podemos usar caminho relativo ou absoluto.
CONFIG_FILE_PATH = '/home/ubuntu/mapping_archives/devices_config/devices.json'

class BaseMappingService(Service):
    """ Classe base com utilitários para ler o arquivo de dispositivos. """
    
    def _load_devices_from_file(self):
        """ Lê e parseia o arquivo JSON de dispositivos. """
        try:
            if not os.path.exists(CONFIG_FILE_PATH):
                self.logger.warning(f"Arquivo de dispositivos não encontrado em: {CONFIG_FILE_PATH}")
                return []

            with open(CONFIG_FILE_PATH, 'r') as f:
                data = json.load(f)
                return data
        except Exception as e:
            self.logger.error(f"Erro ao ler devices.json: {e}")
            return []

    def _send_tatu_command(self, device_id, command):
        """ 
        Envia o comando formatado para o tópico de requisição do dispositivo.
        """
        # No TATU, o tópico de requisição segue o padrão dev/<device_id>
        topic = f'dev/{device_id}'
        
        # Publica usando a conexão MQTT configurada no Zato
        self.out.mqtt.send('mqtt_broker', topic, command)
        self.logger.info(f"Comando TATU enviado para {topic}: {command}")


class GetDevice(BaseMappingService):
    """
    Retorna a configuração completa de um dispositivo específico.
    """
    name = 'soft-iot.mapping.get-device'

    def handle(self):
        # Obtém o ID do payload da requisição (ex: {'device_id': 'sensor1'})
        device_id = self.request.payload.get('device_id')
        
        if not device_id:
            self.response.payload = {'error': 'device_id is required', 'device': None}
            return

        devices = self._load_devices_from_file()
        
        # Busca o dispositivo na lista
        target_device = next((d for d in devices if d.get('id') == device_id), None)

        if target_device:
            self.response.payload = {'status': 'found', 'device': target_device}
        else:
            self.response.payload = {'status': 'not_found', 'device': None}


class ListDevices(BaseMappingService):
    """
    Retorna todos os dispositivos cadastrados.
    """
    name = 'soft-iot.mapping.list-devices'

    def handle(self):
        devices = self._load_devices_from_file()
        self.response.payload = {'total': len(devices), 'devices': devices}


class SendGetCommand(BaseMappingService):
    """
    Solicita uma leitura única de um sensor.
    """
    name = 'soft-iot.mapping.send-get'

    def handle(self):
        device_id = self.request.payload.get('device_id')
        sensor_id = self.request.payload.get('sensor_id')

        if not all([device_id, sensor_id]):
            self.response.payload = {'error': 'device_id and sensor_id are required'}
            return

        # Gera comando: GET INFO <sensor_id>
        command = f'GET INFO {sensor_id}'
        self._send_tatu_command(device_id, command)
        
        self.response.payload = {'status': 'command_sent', 'command': command}


class SendFlowCommand(BaseMappingService):
    """
    Inicia um fluxo contínuo de dados.
    """
    name = 'soft-iot.mapping.send-flow'

    def handle(self):
        device_id = self.request.payload.get('device_id')
        sensor_id = self.request.payload.get('sensor_id')
        collect = self.request.payload.get('collect_time', 10000) # Padrão 10s
        publish = self.request.payload.get('publish_time', 20000) # Padrão 20s

        if not all([device_id, sensor_id]):
            self.response.payload = {'error': 'device_id and sensor_id are required'}
            return

        # Gera comando: FLOW INFO <sensor_id> <collect_time> <publish_time>
        command = f'FLOW INFO {sensor_id} {collect} {publish}'
        self._send_tatu_command(device_id, command)
        
        self.response.payload = {'status': 'flow_initiated', 'command': command}


class SendSetCommand(BaseMappingService):
    """
    Envia um comando de atuação para o dispositivo.
    """
    name = 'soft-iot.mapping.send-set'

    def handle(self):
        device_id = self.request.payload.get('device_id')
        sensor_id = self.request.payload.get('sensor_id')
        value = self.request.payload.get('value')

        if value is None or not all([device_id, sensor_id]):
            self.response.payload = {'error': 'device_id, sensor_id and value are required'}
            return

        # Gera comando: SET INFO <sensor_id> VALUE <value>
        command = f'SET INFO {sensor_id} VALUE {value}'
        self._send_tatu_command(device_id, command)
        
        self.response.payload = {'status': 'action_sent', 'command': command}