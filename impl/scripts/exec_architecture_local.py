import os
import time

def executar_comando(cmd: str):
    """Executa um comando no terminal do sistema operacional"""
    os.system(cmd)

def limpar_ambiente(quant_gateways: int, quant_devices_por_gateway: int):
    """Para e remove todos os contêineres antigos (ZMQ, API, Gateways e Devices)"""
    print("\n[INFO] Parando e removendo contêineres antigos...")
    
    # Limpeza do ZMQ e da API Tangle
    executar_comando("docker stop zmq0 >/dev/null 2>&1")
    executar_comando("docker rm zmq0 >/dev/null 2>&1")
    executar_comando("docker stop api0 >/dev/null 2>&1")
    executar_comando("docker rm api0 >/dev/null 2>&1")
    
    # Limpeza dos Gateways e seus respectivos Devices
    for i in range(quant_gateways):
        executar_comando(f"docker stop gat{i} >/dev/null 2>&1")
        executar_comando(f"docker rm gat{i} >/dev/null 2>&1")
        
        for z in range(quant_devices_por_gateway):
            executar_comando(f"docker stop dev_{i}_{z} >/dev/null 2>&1")
            executar_comando(f"docker rm dev_{i}_{z} >/dev/null 2>&1")
        
    print("[INFO] Limpeza concluída.")

if __name__ == '__main__':
    print("\n=========================================================")
    print(" EXPERIMENTO DOCKER NATIVO (API TANGLE SEPARADA + DEVICES) ")
    print("=========================================================\n")
    
    hornet_ip = "172.17.0.4"
    
    try:
        quant_gateways = int(input("Digite a quantidade de Gateways a serem instanciados: "))
        quant_devices_por_gateway = int(input("Digite a quantidade de Dispositivos por Gateway: "))
    except ValueError:
        print("Quantidade inválida. Encerrando.")
        exit(1)

    # 1. Garante que o ambiente está limpo
    limpar_ambiente(quant_gateways, quant_devices_por_gateway)

    # 2. Instancia o servidor ZMQ
    print("\n[INFO] Iniciando Ponte ZMQ...")
    cmd_zmq = f"""docker run -d --name zmq0 --network bridge \\
        -e MQTT_IP="{hornet_ip}" \\
        -e INDEXES="REP_*" \\
        silviozv/iota-zmq-bridge:1.0.0"""
    executar_comando(cmd_zmq)

    # 3. Instancia a API Go (Separada)
    print("\n[INFO] Iniciando API Tangle (Go)...")
    cmd_api = f"""docker run -d --name api0 --network bridge \\
        -e API_PORT="3000" \\
        -e TANGLE_NODE_URL="{hornet_ip}" \\
        -e TANGLE_NODE_PORT="14265" \\
        silviozv/tangle-hornet-api:1.0.2 \\
        ./entrypoint.sh"""
    executar_comando(cmd_api)

    # 4. Instancia os Gateways Zato com IPs diretos e sequenciais
    print("\n[INFO] Iniciando Gateways (Zato) com IPs reais definidos...")
    for i in range(quant_gateways):
        nome_gat = f"gat{i}"
        gateway_ip = f"172.17.0.{6 + i}"  # gat0 = 172.17.0.6, gat1 = 172.17.0.7, etc.
        
        cmd_gat = f"""docker run -d --name {nome_gat} --network bridge --link zmq0:zmq0 --link api0:api0 \\
            -e Zato_Dashboard_Password="123456" \\
            -e ZATO_SSH_PASSWORD="123456" \\
            -e Zato_IDE_Password="123456" \\
            -e Zato_Log_Env_Details="true" \\
            -e Zato_SAVE_DATA_ENABLED="True" \\
            -e Zato_COLLECTION_TIME="2" \\
            -e Zato_PUBLISH_TIME="6" \\
            -e Zato_AGGREGATION_WINDOW_MINUTES="10" \\
            -e Zato_DATA_RETENTION_SECONDS="1200" \\
            -e Zato_TANGLE_API_IP="api0" \\
            -e Zato_TANGLE_API_PORT="3000" \\
            -e Zato_ZMQ_IP="zmq0" \\
            -e Zato_ZMQ_PORT="5556" \\
            -e Zato_NODE_TYPE="1" \\
            -e Zato_HONESTY_RATE="100" \\
            -e Zato_GROUP="cloud/c1" \\
            -e Zato_GATEWAY_REAL_IP="{gateway_ip}" \\
            silviozv/soft-iot-gateway-zato-vertical:1.0.0 \\
            bash -c "sleep 10 && /usr/local/bin/start_wrapper.sh"
        """
        executar_comando(cmd_gat)
        print(f"[+] Gateway {nome_gat} em execução com IP: {gateway_ip}")

    # 5. Instancia os Dispositivos (Devices)
    print("\n[INFO] Iniciando Dispositivos IoT...")
    for i in range(quant_gateways):
        nome_gat = f"gat{i}"
        
        for z in range(quant_devices_por_gateway):
            nome_dev = f"dev_{i}_{z}"
            
            cmd_dev = f"""docker run -d --name {nome_dev} --network bridge --link {nome_gat}:{nome_gat} \\
                -e BROKER_IP="{nome_gat}" \\
                -e BROKER_PORT="1883" \\
                silviozv/python-iot-device:1.0.0 \\
                bash -c "sleep 120; python -m app.main; tail -f /dev/null"
            """
            executar_comando(cmd_dev)
            print(f"  -> Device {nome_dev} em execução e conectado ao {nome_gat}.")

    print("\n=========================================================")
    print("Ambiente completo iniciado em segundo plano!")
    print("Para monitorar a API Go:  docker logs -f api0")
    print("Para monitorar o Zato:    docker logs -f gat0")
    print("MANTENHA ESTE TERMINAL ABERTO. Pressione Ctrl+C para encerrar tudo.")
    print("=========================================================\n")

    # 6. Mantém o script vivo até o usuário apertar Ctrl+C
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[!] Sinal de interrupção recebido (Ctrl+C).")
        limpar_ambiente(quant_gateways, quant_devices_por_gateway)