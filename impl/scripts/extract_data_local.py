import subprocess
import requests
import json
import os
import csv
from datetime import datetime

# ==========================================
# CONFIGURAÇÕES
# ==========================================
# Nome exato da imagem Docker dos seus gateways
IMAGEM_GATEWAY = "silviozv/soft-iot-gateway-zato-vertical:1.0.0"

# A porta interna do Zato onde o serviço REST está rodando
PORTA_ZATO = "11223"

# A rota exata do seu serviço Zato que devolve o JSON local
ENDPOINT_EXTRACAO = "/soft-iot/reputation/task/get-data"

# Diretório base onde as coleções serão acumuladas
DIRETORIO_SAIDA = "data"

# Prefixo do arquivo JSON bruto
PREFIXO_ARQUIVO = "backup_gateways_data_"

def obter_ips_dos_gateways():
    """Busca no Docker todos os containers usando a imagem do gateway e retorna seus IPs."""
    print(f"[*] Buscando containers com a imagem: {IMAGEM_GATEWAY}...")
    ips_encontrados = []
    
    try:
        cmd_ids = f"docker ps -q --filter ancestor={IMAGEM_GATEWAY}"
        resultado_ids = subprocess.check_output(cmd_ids, shell=True).decode('utf-8').strip().split('\n')
        ids = [cid for cid in resultado_ids if cid]

        if not ids:
            print("[-] Nenhum gateway em execução encontrado.")
            return ips_encontrados

        print(f"[+] Encontrados {len(ids)} gateways em execução. Extraindo IPs...")

        for cid in ids:
            cmd_ip = f"docker inspect -f '{{{{range.NetworkSettings.Networks}}}}{{{{.IPAddress}}}}{{{{end}}}}' {cid}"
            ip = subprocess.check_output(cmd_ip, shell=True).decode('utf-8').strip()
            
            cmd_nome = f"docker inspect -f '{{{{.Name}}}}' {cid}"
            nome = subprocess.check_output(cmd_nome, shell=True).decode('utf-8').strip().replace('/', '')
            
            if ip:
                ips_encontrados.append({"nome": nome, "ip": ip})

    except Exception as e:
        print(f"[!] Erro ao comunicar com o Docker: {e}")
        
    return ips_encontrados

def extrair_dados(lista_gateways):
    """Faz a requisição HTTP para cada IP e consolida os resultados."""
    dados_consolidados = {
        "data_extracao": datetime.now().isoformat(),
        "total_gateways": len(lista_gateways),
        "gateways": {}
    }

    for gateway in lista_gateways:
        nome = gateway['nome']
        ip = gateway['ip']
        url = f"http://{ip}:{PORTA_ZATO}{ENDPOINT_EXTRACAO}"
        
        print(f"[*] Requisitando dados de {nome} ({url})...")
        
        try:
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                try:
                    conteudo = response.json()
                except json.JSONDecodeError:
                    conteudo = response.text
                
                dados_consolidados['gateways'][nome] = {
                    "ip": ip,
                    "status": "sucesso",
                    "dados": conteudo
                }
                print(f"  [+] Dados de {nome} coletados com sucesso.")
            else:
                dados_consolidados['gateways'][nome] = {
                    "ip": ip,
                    "status": "erro_http",
                    "codigo": response.status_code
                }
                print(f"  [-] {nome} retornou erro HTTP {response.status_code}.")
                
        except requests.exceptions.RequestException as e:
            dados_consolidados['gateways'][nome] = {
                "ip": ip,
                "status": "falha_conexao",
                "erro": str(e)
            }
            print(f"  [!] Falha de rede ao conectar com {nome}.")

    return dados_consolidados

def obter_proximo_diretorio():
    """Garante a existência do diretório base e cria uma subpasta sequencial (collect_N)."""
    os.makedirs(DIRETORIO_SAIDA, exist_ok=True)
    
    numeros_coleta = []
    
    # Varre as pastas dentro do diretório de saída
    for nome_pasta in os.listdir(DIRETORIO_SAIDA):
        caminho_pasta = os.path.join(DIRETORIO_SAIDA, nome_pasta)
        if os.path.isdir(caminho_pasta) and nome_pasta.startswith("collect_"):
            try:
                # Extrai o número da pasta (ex: "collect_5" -> 5)
                numero = int(nome_pasta.split("_")[1])
                numeros_coleta.append(numero)
            except ValueError:
                pass
                
    # Determina o próximo número
    proximo_numero = max(numeros_coleta) + 1 if numeros_coleta else 1
    
    # Cria o novo diretório
    novo_diretorio = os.path.join(DIRETORIO_SAIDA, f"collect_{proximo_numero}")
    os.makedirs(novo_diretorio, exist_ok=True)
    
    return novo_diretorio

def salvar_dados(dados):
    """Escreve o JSON consolidado e gera os arquivos CSV individuais por gateway no novo diretório."""
    diretorio_atual = obter_proximo_diretorio()
    print(f"\n[📁] Diretório da execução criado: {diretorio_atual}")
    
    # 1. Salvar o arquivo JSON consolidado
    timestamp_atual = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo_json = f"{PREFIXO_ARQUIVO}{timestamp_atual}.json"
    caminho_json = os.path.join(diretorio_atual, nome_arquivo_json)
    
    try:
        with open(caminho_json, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=4)
        print(f"  [📄] JSON salvo: {nome_arquivo_json}")
    except Exception as e:
        print(f"  [!] Erro ao salvar o JSON: {e}")

    # 2. Gerar arquivos CSV para cada gateway com base no requests_history
    for nome_gateway, info_gateway in dados.get("gateways", {}).items():
        if info_gateway.get("status") == "sucesso":
            try:
                # Navega até a lista de requisições baseada na estrutura fornecida
                historico_requisicoes = info_gateway["dados"]["data"]["requests_history"]
                
                if not historico_requisicoes:
                    print(f"  [!] {nome_gateway}: A lista 'requests_history' está vazia. CSV ignorado.")
                    continue
                
                nome_arquivo_csv = f"{nome_gateway}_requests.csv"
                caminho_csv = os.path.join(diretorio_atual, nome_arquivo_csv)
                
                # As colunas serão exatamente as chaves do primeiro dicionário na lista
                colunas = historico_requisicoes[0].keys()
                
                with open(caminho_csv, 'w', newline='', encoding='utf-8') as f_csv:
                    escritor = csv.DictWriter(f_csv, fieldnames=colunas)
                    escritor.writeheader()
                    escritor.writerows(historico_requisicoes)
                    
                print(f"  [📊] CSV gerado: {nome_arquivo_csv}")
                
            except KeyError:
                print(f"  [!] {nome_gateway}: Estrutura de dados não possui 'requests_history'.")
            except Exception as e:
                print(f"  [!] {nome_gateway}: Erro ao gerar o CSV - {e}")

    print(f"\n[🚀] Extração finalizada com sucesso no diretório: {os.path.abspath(diretorio_atual)}")

if __name__ == "__main__":
    gateways = obter_ips_dos_gateways()
    if gateways:
        resultados = extrair_dados(gateways)
        salvar_dados(resultados)