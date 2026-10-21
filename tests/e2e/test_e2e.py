# tests/e2e/test_e2e.py
"""
E2E contra um experimento Fogbed já em execução.

  GATEWAYS="http://10.0.0.x:11223,http://10.0.0.y:11223,..." \
  MIN_REQUESTS=10 pytest tests/e2e -v -s

Variáveis opcionais:
  E2E_TIMEOUT=900                 tempo máximo esperando os gateways acumularem requisições
  EXPECT_ALL_HONEST=1             exige zero falhas de consumo (cenário só com honestos)
  EXPECT_LOWEST_REPUTATION=<url>  este gateway (o malicioso) deve ter a menor reputação
"""
import os
import time

import pytest
import requests

GATEWAYS = [g.strip().rstrip('/') for g in os.environ.get('GATEWAYS', '').split(',') if g.strip()]
MIN_REQUESTS = int(os.environ.get('MIN_REQUESTS', '10'))
TIMEOUT = int(os.environ.get('E2E_TIMEOUT', '900'))
PORT = 11223
TERMINAL = {"FINISHED", "FAILED", "FAILED_NO_SERVICES", "FAILED_DATA_CONSUMPTION", "TIMEOUT"}

pytestmark = pytest.mark.skipif(not GATEWAYS, reason="defina GATEWAYS com as URLs dos gateways")


def fetch_state(base):
    r = requests.get(f"{base}/soft-iot/reputation/task/get-data", timeout=15)
    r.raise_for_status()
    return r.json()["data"]


def finished_count(st):
    return sum(1 for r in st["requests_history"] if r["status"] == "FINISHED")


@pytest.fixture(scope="session")
def states():
    """Espera todos os gateways acumularem MIN_REQUESTS requisições FINISHED e devolve o estado de cada um."""
    deadline = time.time() + TIMEOUT
    last = {}
    while time.time() < deadline:
        try:
            last = {g: fetch_state(g) for g in GATEWAYS}
            if all(finished_count(st) >= MIN_REQUESTS for st in last.values()):
                return last
        except requests.RequestException:
            pass
        time.sleep(15)

    resumo = {g: finished_count(st) for g, st in last.items()} or "nenhum gateway respondeu"
    pytest.fail(f"Timeout: requisições FINISHED por gateway = {resumo} (esperado >= {MIN_REQUESTS})")


def own_id(st):
    return st["requests_history"][0]["id_evaluator"]


def test_todos_os_gateways_respondem(states):
    assert len(states) == len(GATEWAYS)


def test_gateways_descobrem_uns_aos_outros(states):
    """Cada gateway recebeu ofertas de todos os outros: Tangle + ponte ZMQ funcionando."""
    ids = {g: own_id(st) for g, st in states.items()}
    for g, st in states.items():
        responders = {r["source"] for r in st["responses_history"]}
        esperados = {i for gg, i in ids.items() if gg != g}
        faltando = esperados - responders
        assert not faltando, f"{g} nunca recebeu oferta de: {faltando}"


def test_dados_dos_devices_chegam_via_gateway_par(states):
    """device -> gateway (TATU/MQTT/SQLite) -> outro gateway (HTTP direto)."""
    for g, st in states.items():
        ofertas = {}
        for r in st["responses_history"]:
            ofertas[r["source"]] = r            # fica com a oferta mais recente de cada par

        ok = False
        for r in ofertas.values():
            for svc in r["services"]:
                url = (f"http://{r['ip_source']}:{PORT}/soft-iot/devices/"
                       f"{svc['device_id']}/sensors/{svc['sensor_id']}/data/latest")
                try:
                    resp = requests.get(url, timeout=10)
                except requests.RequestException:
                    continue
                if resp.status_code == 200:
                    valor = float(resp.json()["data"]["data_value"])
                    assert 0.0 <= valor <= 100.0
                    ok = True
        assert ok, f"{g} não conseguiu ler dado real de nenhum par"


def test_ciclo_completo_com_credibilidade(states):
    for g, st in states.items():
        finalizadas = [r for r in st["requests_history"] if r["status"] == "FINISHED"]
        assert finalizadas
        for r in finalizadas:
            assert r["id_provider"] is not None
            assert r["old_credibility_evaluator"] is not None
            assert r["new_credibility_evaluator"] is not None
            assert r["reputation_provider"] is not None


def test_avaliacoes_circulam_pela_tangle(states):
    """Se alguma reputação saiu de 0,5, houve escrita e leitura de avaliações na Tangle."""
    reputacoes = [float(st["gateway_properties"]["reputation"]) for st in states.values()]
    assert any(abs(r - 0.5) > 1e-6 for r in reputacoes), f"reputações paradas em 0,5: {reputacoes}"


def test_sem_requisicoes_travadas(states):
    for g, st in states.items():
        reqs = st["requests_history"]
        timeouts = sum(1 for r in reqs if r["status"] == "TIMEOUT")
        assert timeouts <= max(1, len(reqs) // 10), f"{g}: {timeouts} TIMEOUT em {len(reqs)}"
        for r in reqs[:-1]:                     # só a última pode estar em andamento
            assert r["status"] in TERMINAL, f"{g}: requisição {r['id']} presa em {r['status']}"


@pytest.mark.skipif(os.environ.get('EXPECT_ALL_HONEST') != '1', reason="cenário não é só de honestos")
def test_cenario_honesto_nao_tem_falha_de_consumo(states):
    for g, st in states.items():
        falhas = [r for r in st["requests_history"] if r["status"] == "FAILED_DATA_CONSUMPTION"]
        assert not falhas, f"{g}: {len(falhas)} falhas de consumo"


@pytest.mark.skipif(not os.environ.get('EXPECT_LOWEST_REPUTATION'), reason="sem gateway malicioso indicado")
def test_malicioso_tem_a_menor_reputacao(states):
    alvo = os.environ['EXPECT_LOWEST_REPUTATION'].rstrip('/')
    reps = {g: float(st["gateway_properties"]["reputation"]) for g, st in states.items()}
    assert min(reps, key=reps.get) == alvo, f"reputações: {reps}"