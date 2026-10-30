# ==========================================
# ESTÁGIO 1: Compilação da API Tangle em Go
# ==========================================
FROM golang:1.20 AS builder

# Versão (tag) da API no GitHub
ARG API_VERSION=v1.0.1

WORKDIR /app

# Baixa o código da API direto do GitHub, na tag escolhida
RUN git clone --depth 1 --branch ${API_VERSION} https://github.com/larsid/tangle-hornet-api.git .

# Instala dependências e compila o binário estaticamente
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o api-tangle main.go


# ==========================================
# ESTÁGIO 2: Montagem do Ambiente do Gateway
# ==========================================
FROM rhianpablo11/esb-zato-soft-iot:v12

USER root

# Cria a infraestrutura de diretórios
RUN mkdir -p /opt/hot-deploy/myproject \
             /opt/hot-deploy/enmasse \
             /opt/hot-deploy/python-reqs \
             /home/ubuntu/mapping_archives/devices_config/ \
             /opt/api-tangle/config

# Copia os arquivos do Zato
COPY . /opt/hot-deploy/myproject/
COPY config/enmasse/enmasse.yaml /opt/hot-deploy/enmasse/enmasse.yaml
COPY config/python-reqs/requirements.txt /opt/hot-deploy/python-reqs/requirements.txt
COPY impl/src/archives/. /home/ubuntu/mapping_archives/devices_config/

# Copia o env.ini estático (Gere-o antes de buildar)
COPY config/auto-generated/env.ini /opt/hot-deploy/enmasse/env.ini

# Limpeza
RUN rm -f /opt/hot-deploy/myproject/impl/scripts/fogbed-test.py

# --- Integração do Sidecar (API Go) ---
# Extrai o executável do estágio 1
COPY --from=builder /app/api-tangle /usr/local/bin/api-tangle

# --- Script de Entrada Dinâmico ---
# Cria o script entrypoint.sh que lê as variáveis de ambiente no RUNTIME (docker run -e)
# e escreve o tangle-hornet.conf atualizado antes de iniciar os serviços
RUN cat << 'EOF' > /usr/local/bin/entrypoint.sh
#!/bin/bash
set -e

# Garante a existência do diretório para o log e para a configuração
mkdir -p /opt/api-tangle
mkdir -p /etc

# Escreve a configuração na pasta /etc/ (onde a API Go procura)
cat <<EOC > /etc/tangle-hornet.conf
apiPort = ${API_PORT:-3000}
nodeUrl = ${TANGLE_NODE_URL:-127.0.0.1}
nodePort = ${TANGLE_NODE_PORT:-14265}
EOC

# Inicia a API Go em segundo plano
cd /opt/api-tangle && /usr/local/bin/api-tangle > /opt/api-tangle/api.log 2>&1 &

# Transfere a execução para o serviço principal do Zato
exec /usr/local/bin/start_wrapper.sh
EOF

# Ajusta permissões de execução e propriedade dos arquivos
RUN chmod +x /usr/local/bin/entrypoint.sh && \
    chmod +x /usr/local/bin/api-tangle && \
    chown -R zato:zato /opt/hot-deploy/ /home/ubuntu/mapping_archives/ /opt/api-tangle/ /usr/local/bin/entrypoint.sh

# Ponto de entrada do container
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]