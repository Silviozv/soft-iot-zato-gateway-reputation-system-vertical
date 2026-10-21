#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Executa N rodadas do experimento (ZMQ + API + Gateways + Devices), cada uma por X minutos,
e coleta os dados de cada rodada em data_relatorio_final/run_N.

A Tangle (Hornet) NAO é iniciada aqui: suba-a antes, manualmente.

Uso:
    python3 executar_experimentos.py                      # usa os valores padrão abaixo
    python3 executar_experimentos.py --runs 30 --minutes 40
    python3 executar_experimentos.py --name "30pct_maliciosos"

Recomendado rodar dentro de tmux/screen ou com nohup (30 x 40 min = 20h+).
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime

import requests

# =====================================================================
# CONFIGURAÇÃO DO EXPERIMENTO  (edite aqui)
# =====================================================================

# IP do nó Hornet da Tangle (já em execução)
HORNET_IP = "172.17.0.4"

# Um item por gateway. O número de itens define a quantidade de gateways.
#   node_type     -> vai para Zato_NODE_TYPE
#   honesty_rate  -> vai para Zato_HONESTY_RATE
GATEWAYS = [
    {"node_type": 1, "honesty_rate": 100},
    {"node_type": 1, "honesty_rate": 100},
    {"node_type": 1, "honesty_rate": 100},
    {"node_type": 2, "honesty_rate": 30},   
]

DEVICES_PER_GATEWAY = 1

# Execução
RUNS = 5                      # quantidade de rodadas
DURATION_MINUTES = 100          # tempo de cada rodada (contado após subir os containers)
WARMUP_SECONDS = 0             # tempo extra de aquecimento antes de contar os minutos
COOLDOWN_SECONDS = 30          # pausa entre rodadas
MAX_RETRIES_PER_RUN = 2        # se a subida falhar (ex.: IP inesperado), tenta de novo
EXPERIMENT_NAME = "experimento"  # só informativo, vai para o meta.json

# Coleta
OUTPUT_DIR = "data_relatorio_final"
ZATO_PORT = "11223"
EXTRACT_ENDPOINT = "/soft-iot/reputation/task/get-data"
SAVE_DOCKER_LOGS = True        # salva `docker logs` de cada gateway junto com os dados

# Imagens
IMG_ZMQ = "silviozv/iota-zmq-bridge:1.0.0"
IMG_API = "silviozv/tangle-hornet-api:1.0.2"
IMG_GATEWAY = "silviozv/soft-iot-gateway-zato-vertical:1.0.0"
IMG_DEVICE = "silviozv/python-iot-device:1.0.0"

# gat0 = 172.17.0.{IP_BASE}, gat1 = 172.17.0.{IP_BASE+1}, ...
# (a bridge padrão do Docker não aceita --ip, então o IP é verificado após subir)
IP_BASE = 6


# =====================================================================
# UTILITÁRIOS DOCKER
# =====================================================================

def docker(*args, capture=False):
    """Executa `docker ...` sem usar shell."""
    cmd = ["docker", *args]
    if capture:
        return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                              text=True).stdout.strip()
    return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def nomes_containers():
    nomes = ["zmq0", "api0"]
    for i in range(len(GATEWAYS)):
        nomes.append(f"gat{i}")
        nomes += [f"dev_{i}_{z}" for z in range(DEVICES_PER_GATEWAY)]
    return nomes


def limpar_ambiente():
    print("[INFO] Removendo contêineres (zmq, api, gateways, devices)...")
    docker("rm", "-f", *nomes_containers())


def ip_do_container(nome):
    return docker(
        "inspect", "-f",
        "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
        nome, capture=True,
    )


def container_rodando(nome):
    return docker("inspect", "-f", "{{.State.Running}}", nome, capture=True) == "true"


# =====================================================================
# SUBIDA DO AMBIENTE
# =====================================================================

def iniciar_zmq():
    docker("run", "-d", "--name", "zmq0", "--network", "bridge",
           "-e", f"MQTT_IP={HORNET_IP}",
           "-e", "INDEXES=REP_*",
           IMG_ZMQ)


def iniciar_api():
    docker("run", "-d", "--name", "api0", "--network", "bridge",
           "-e", "API_PORT=3000",
           "-e", f"TANGLE_NODE_URL={HORNET_IP}",
           "-e", "TANGLE_NODE_PORT=14265",
           IMG_API, "./entrypoint.sh")


def iniciar_gateway(i, cfg):
    nome = f"gat{i}"
    ip_esperado = f"172.17.0.{IP_BASE + i}"
    env = {
        "Zato_Dashboard_Password": "123456",
        "ZATO_SSH_PASSWORD": "123456",
        "Zato_IDE_Password": "123456",
        "Zato_Log_Env_Details": "true",
        "Zato_SAVE_DATA_ENABLED": "True",
        "Zato_COLLECTION_TIME": "2",
        "Zato_PUBLISH_TIME": "6",
        "Zato_AGGREGATION_WINDOW_MINUTES": "10",
        "Zato_DATA_RETENTION_SECONDS": "1200",
        "Zato_TANGLE_API_IP": "api0",
        "Zato_TANGLE_API_PORT": "3000",
        "Zato_ZMQ_IP": "zmq0",
        "Zato_ZMQ_PORT": "5556",
        "Zato_NODE_TYPE": str(cfg["node_type"]),
        "Zato_HONESTY_RATE": str(cfg["honesty_rate"]),
        "Zato_GROUP": "cloud/c1",
        "Zato_GATEWAY_REAL_IP": ip_esperado,
    }
    args = ["run", "-d", "--name", nome, "--network", "bridge",
            "--link", "zmq0:zmq0", "--link", "api0:api0"]
    for k, v in env.items():
        args += ["-e", f"{k}={v}"]
    args += [IMG_GATEWAY, "bash", "-c", "sleep 10 && /usr/local/bin/start_wrapper.sh"]
    docker(*args)
    return nome, ip_esperado


def iniciar_device(i, z):
    nome_gat = f"gat{i}"
    docker("run", "-d", "--name", f"dev_{i}_{z}", "--network", "bridge",
           "--link", f"{nome_gat}:{nome_gat}",
           "-e", f"BROKER_IP={nome_gat}",
           "-e", "BROKER_PORT=1883",
           IMG_DEVICE,
           "bash", "-c", "sleep 120; python -m app.main; tail -f /dev/null")


def subir_ambiente():
    """Sobe tudo. Retorna True se os IPs dos gateways baterem com o esperado."""
    limpar_ambiente()

    print("[INFO] Iniciando ZMQ e API...")
    iniciar_zmq()
    iniciar_api()

    print("[INFO] Iniciando gateways...")
    for i, cfg in enumerate(GATEWAYS):
        nome, ip_esperado = iniciar_gateway(i, cfg)
        print(f"  [+] {nome}  tipo={cfg['node_type']}  honestidade={cfg['honesty_rate']}  "
              f"IP esperado={ip_esperado}")

    ok = True
    for i in range(len(GATEWAYS)):
        nome, esperado = f"gat{i}", f"172.17.0.{IP_BASE + i}"
        real = ip_do_container(nome)
        if real != esperado:
            print(f"  [!] {nome}: IP real {real or 'N/D'} != esperado {esperado}")
            ok = False
    if not ok:
        return False

    print("[INFO] Iniciando devices...")
    for i in range(len(GATEWAYS)):
        for z in range(DEVICES_PER_GATEWAY):
            iniciar_device(i, z)
    return True


# =====================================================================
# ESPERA E COLETA
# =====================================================================

def aguardar(segundos, rotulo):
    fim = time.time() + segundos
    while True:
        restante = fim - time.time()
        if restante <= 0:
            break
        print(f"\r  [{rotulo}] restante {int(restante // 60):02d}:{int(restante % 60):02d}   ",
              end="", flush=True)
        time.sleep(min(30, restante))
    print()


def reservar_pasta_run():
    """Cria data_relatorio_final/run_N com N = maior existente + 1. Nunca reutiliza pasta."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    while True:
        existentes = []
        for d in os.listdir(OUTPUT_DIR):
            m = re.fullmatch(r"run_(\d+)", d)
            if m and os.path.isdir(os.path.join(OUTPUT_DIR, d)):
                existentes.append(int(m.group(1)))
        n = max(existentes, default=0) + 1
        caminho = os.path.join(OUTPUT_DIR, f"run_{n}")
        try:
            os.makedirs(caminho, exist_ok=False)  # falha se já existir -> não sobrescreve
            return n, caminho
        except FileExistsError:
            continue


def requisitar_gateway(nome, tentativas=3):
    ip = ip_do_container(nome)
    if not ip:
        return {"status": "sem_ip"}
    url = f"http://{ip}:{ZATO_PORT}{EXTRACT_ENDPOINT}"
    for t in range(1, tentativas + 1):
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                try:
                    return {"ip": ip, "status": "sucesso", "dados": r.json()}
                except json.JSONDecodeError:
                    return {"ip": ip, "status": "sucesso", "dados": r.text}
            erro = f"HTTP {r.status_code}"
        except requests.exceptions.RequestException as e:
            erro = str(e)
        print(f"  [!] {nome}: tentativa {t}/{tentativas} falhou ({erro})")
        time.sleep(10)
    return {"ip": ip, "status": "falha", "erro": erro}


def coletar_dados(pasta):
    """Coleta dados de todos os gateways e grava na pasta (já exclusiva da rodada)."""
    consolidado = {
        "data_extracao": datetime.now().isoformat(),
        "total_gateways": len(GATEWAYS),
        "gateways": {},
    }

    for i in range(len(GATEWAYS)):
        nome = f"gat{i}"
        print(f"  [*] Coletando {nome}...")
        info = requisitar_gateway(nome)
        info["config"] = GATEWAYS[i]
        consolidado["gateways"][nome] = info

        if SAVE_DOCKER_LOGS:
            with open(os.path.join(pasta, f"{nome}.log"), "w", encoding="utf-8") as f:
                subprocess.run(["docker", "logs", nome], stdout=f, stderr=subprocess.STDOUT)

        if info["status"] != "sucesso":
            continue
        try:
            historico = info["dados"]["data"]["requests_history"]
        except (KeyError, TypeError):
            print(f"  [!] {nome}: sem 'requests_history'.")
            continue
        if not historico:
            print(f"  [!] {nome}: 'requests_history' vazio.")
            continue

        colunas = []
        for linha in historico:
            for k in linha.keys():
                if k not in colunas:
                    colunas.append(k)
        with open(os.path.join(pasta, f"{nome}_requests.csv"), "w",
                  newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=colunas)
            w.writeheader()
            w.writerows(historico)

    with open(os.path.join(pasta, "dados_consolidados.json"), "w", encoding="utf-8") as f:
        json.dump(consolidado, f, ensure_ascii=False, indent=4)

    return {n: g["status"] for n, g in consolidado["gateways"].items()}


# =====================================================================
# RODADA E LOOP PRINCIPAL
# =====================================================================

def executar_rodada(minutos, indice, total):
    print(f"\n{'=' * 60}\n RODADA {indice}/{total}  ({datetime.now():%Y-%m-%d %H:%M:%S})\n{'=' * 60}")

    subiu = False
    for tentativa in range(1, MAX_RETRIES_PER_RUN + 2):
        if subir_ambiente():
            subiu = True
            break
        print(f"[!] Falha ao subir o ambiente (tentativa {tentativa}). Limpando e repetindo...")
        limpar_ambiente()
        time.sleep(10)
    if not subiu:
        return None

    inicio = datetime.now()
    if WARMUP_SECONDS:
        aguardar(WARMUP_SECONDS, "aquecimento")
    aguardar(minutos * 60, "execução")
    fim = datetime.now()

    print("[INFO] Coletando dados...")
    n, pasta = reservar_pasta_run()
    resultado = coletar_dados(pasta)

    status = "completo" if all(s == "sucesso" for s in resultado.values()) else "parcial"
    meta = {
        "run": n,
        "experimento": EXPERIMENT_NAME,
        "status": status,
        "inicio": inicio.isoformat(),
        "fim": fim.isoformat(),
        "duracao_minutos": minutos,
        "warmup_segundos": WARMUP_SECONDS,
        "devices_por_gateway": DEVICES_PER_GATEWAY,
        "gateways": GATEWAYS,
        "coleta": resultado,
    }
    with open(os.path.join(pasta, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=4)

    print(f"[OK] Rodada salva em {os.path.abspath(pasta)}  (status: {status})")
    return {"run": n, "status": status}


def main():
    global EXPERIMENT_NAME
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=RUNS)
    ap.add_argument("--minutes", type=float, default=DURATION_MINUTES)
    ap.add_argument("--name", default=EXPERIMENT_NAME)
    args = ap.parse_args()
    EXPERIMENT_NAME = args.name

    print(f"Experimento '{EXPERIMENT_NAME}': {args.runs} rodadas x {args.minutes} min, "
          f"{len(GATEWAYS)} gateways, {DEVICES_PER_GATEWAY} devices/gateway")
    for i, g in enumerate(GATEWAYS):
        print(f"  gat{i}: Zato_NODE_TYPE={g['node_type']}  Zato_HONESTY_RATE={g['honesty_rate']}")

    resumo = []
    try:
        for r in range(1, args.runs + 1):
            res = executar_rodada(args.minutes, r, args.runs)
            resumo.append(res or {"run": None, "status": "falha_subida"})
            limpar_ambiente()
            if r < args.runs:
                print(f"[INFO] Pausa de {COOLDOWN_SECONDS}s...")
                time.sleep(COOLDOWN_SECONDS)
    except KeyboardInterrupt:
        print("\n[!] Interrompido (Ctrl+C).")
    finally:
        limpar_ambiente()

    print("\n================ RESUMO ================")
    for i, r in enumerate(resumo, 1):
        print(f"  tentativa {i}: pasta run_{r['run']}  status={r['status']}")


if __name__ == "__main__":
    if subprocess.run(["docker", "info"], stdout=subprocess.DEVNULL,
                      stderr=subprocess.DEVNULL).returncode != 0:
        sys.exit("Docker não está acessível (rode com permissão para usar o docker).")
    main()