from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# ---------------- Configuração ----------------
PASTA = Path("data_relatorio_final/run_1")
GATEWAYS = ["gat0", "gat1", "gat2", "gat3"]
EIXO_X = "id"  # "id" (nº da requisição) ou "tempo" (start_request_time)
SAIDA = PASTA / "evolucao_reputacao_gateways.png"
# -----------------------------------------------


def carregar(gateway: str) -> pd.DataFrame | None:
    caminho = PASTA / f"{gateway}_requests.csv"
    if not caminho.exists():
        print(f"[aviso] arquivo não encontrado: {caminho}")
        return None

    df = pd.read_csv(
        caminho,
        parse_dates=["start_request_time", "finish_request_time"],
    )
    # Requisições FAILED não possuem reputation_evaluator
    df = df.dropna(subset=["reputation_evaluator"]).sort_values("id")
    return df


def main():
    fig, ax = plt.subplots(figsize=(11, 6))

    for gw in GATEWAYS:
        df = carregar(gw)
        if df is None or df.empty:
            continue

        if EIXO_X == "tempo":
            x = df["start_request_time"]
        else:
            x = df["id"]

        ax.plot(x, df["reputation_evaluator"], marker="o", markersize=4, label=gw)

    ax.set_title("Evolução da reputação dos gateways (reputation_evaluator)")
    ax.set_xlabel("Tempo" if EIXO_X == "tempo" else "Nº da requisição (id)")
    ax.set_ylabel("Reputação")
    ax.set_ylim(0, 1.05)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(title="Gateway")

    if EIXO_X == "tempo":
        fig.autofmt_xdate()

    fig.tight_layout()
    fig.savefig(SAIDA, dpi=150)
    print(f"Gráfico salvo em: {SAIDA}")
    plt.show()


if __name__ == "__main__":
    main()