"""
Sobe uma Tangle privada (IOTA 1.5 / Chrysalis, Hornet 1.2) local com o Fogbed:
1 nó (node0), 1 coordenador e 1 spammer.

A Tangle Hornet API não é criada aqui: ela vem embutida em cada gateway.
Depois de subir, use o IP do node0 impresso no final como HORNET_IP no
exec_architecture_local_continuous.py.
"""

import os
import json

from fogledgerIota.iota.IotaBasic import IotaBasic
from fogledgerIota.iota.config.NodeConfig import NodeConfig
from fogbed import FogbedExperiment, setLogLevel


# =====================================================================
# CONFIGURAÇÃO DA TANGLE (edite aqui)
# =====================================================================

# Dificuldade da prova de trabalho (PoW). O padrão do fogledgerIota é 2000.
# A API embutida no gateway faz o PoW localmente com o valor informado pelo nó;
# com 4 gateways numa mesma máquina, 2000 satura a CPU e as escritas passam de 10 s.
# Com 100, cada escrita custa ~8x menos CPU. Em rede privada emulada o PoW só serve
# contra spam, então reduzir não altera a lógica do experimento.
MIN_POW_SCORE = 100

# Máximo de mensagens retornadas por consulta de índice (padrão do Hornet: 1000).
# O histórico de avaliações de um nó é lido inteiro; 1000 limitaria rodadas longas.
MAX_RESULTS = 50000

# Profundidade dos snapshots (padrão do template: 50)
SNAPSHOT_DEPTH = 500

# Pruning: mensagens mais antigas que isso são apagadas (com o coordenador a cada
# 60 s, 5000 milestones equivalem a ~83 horas, muito mais que uma rodada).
MAX_MILESTONES_TO_KEEP = 5000


def patch_hornet_templates() -> None:
    """
    Sobrescreve os templates do Hornet em /tmp/iota/$USER/config/.
    Deve ser chamado DEPOIS de IotaBasic() (que gera os templates) e
    ANTES de iota.start_network() (que os copia para os containers).
    """
    user = os.getenv("USER")
    if not user:
        raise RuntimeError("Variável de ambiente USER não definida.")

    config_dir = f"/tmp/iota/{user}/config"
    template_files = (
        "config-node.json",
        "config-coo.json",
        "config-spammer.json",
    )

    for fname in template_files:
        path = os.path.join(config_dir, fname)

        if not os.path.isfile(path):
            raise FileNotFoundError(
                f"Template Hornet não encontrado: {path}. "
                "Chame patch_hornet_templates() depois de IotaBasic()."
            )

        with open(path, encoding="utf-8") as f:
            cfg = json.load(f)

        # 1. Dificuldade do PoW (precisa ser igual em todos os nós da rede)
        cfg.setdefault("protocol", {})["minPoWScore"] = MIN_POW_SCORE

        # 2. Limite de resultados da API REST e profundidade dos snapshots
        cfg.setdefault("restAPI", {}).setdefault("limits", {})["maxResults"] = MAX_RESULTS
        cfg.setdefault("snapshots", {})["depth"] = SNAPSHOT_DEPTH

        # 3. Spammer desligado: ele faria PoW continuamente (até 5 msg/s, 80% de CPU)
        if fname == "config-spammer.json":
            cfg.setdefault("spammer", {})["autostart"] = False
            cfg["spammer"]["mpsRateLimit"] = 0.0

        # 4. Plugins de rede pública desligados (a rede é privada e com peering fixo).
        #    O MQTT do node0 continua ligado: o gateway depende dele.
        node_cfg = cfg.setdefault("node", {})
        enabled = node_cfg.setdefault("enablePlugins", [])
        disabled = node_cfg.setdefault("disablePlugins", [])
        for plugin in ("Autopeering", "P2PDiscovery", "VersionCheck"):
            if plugin in enabled:
                enabled.remove(plugin)
            if plugin not in disabled:
                disabled.append(plugin)

        # 5. Revalida o banco ao iniciar, caso o processo tenha morrido sem fechar
        cfg.setdefault("db", {})["autoRevalidation"] = True

        # 6. Mais tolerância a atraso do coordenador (template: 120s)
        cfg.setdefault("tangle", {})["milestoneTimeout"] = "300s"

        # 7. Pruning para execuções longas (disco e memória)
        cfg["pruning"] = {
            "milestones": {
                "enabled": True,
                "maxMilestonesToKeep": MAX_MILESTONES_TO_KEEP
            },
            "size": {
                "enabled": True,
                "targetSize": "2GB",
                "thresholdPercentage": 10.0,
                "cooldownTime": "5m"
            },
            "pruneReceipts": False
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=4)

    print(
        f"[INFO] Templates do Hornet ajustados: minPoWScore={MIN_POW_SCORE}, "
        f"maxResults={MAX_RESULTS}, snapshots.depth={SNAPSHOT_DEPTH}, spammer desligado, "
        f"pruning ativo (maxMilestonesToKeep={MAX_MILESTONES_TO_KEEP})."
    )


setLogLevel('info')

if __name__ == '__main__':

    exp = FogbedExperiment()

    # Portas no host: 1881 = dashboard, 1894 = MQTT (eventos), 14265 = API REST
    node0 = NodeConfig(name='node0', port_bindings={'8081': '1881', '1883': '1894', '14265': '14265'})

    # Coordenador e spammer usam as configurações padrão do fogledgerIota
    # (coordenador emite um milestone a cada 60 s)
    iota = IotaBasic(exp=exp, prefix='iota1', conf_nodes=[node0])

    print("Quantidade de instâncias virtuais: ", len(iota.ledgers))

    # Liga node0 -> coordenador -> spammer
    exp.add_link(iota.ledgers[0], iota.ledgers[1])
    exp.add_link(iota.ledgers[1], iota.ledgers[2])

    # Ajusta os templates entre a criação (IotaBasic) e a inicialização (start_network)
    patch_hornet_templates()

    try:
        exp.start()
        iota.start_network()

        print("\n===========================================")
        print("Tangle local iniciada com sucesso!")
        print(f"IP do nó Hornet (use como HORNET_IP): {iota.containers['node0'].ip}")
        print(f"minPoWScore: {MIN_POW_SCORE}")
        print("===========================================\n")

        while True:
            key = input("\nPressione 'p' para parar o experimento: ")
            if key.lower() == 'p':
                print("Parando o experimento...")
                exp.stop()
                break
            else:
                print(f"Entrada ignorada: {key}")
    except Exception as ex:
        print(f"Erro crítico: {ex}")
    finally:
        exp.stop()
