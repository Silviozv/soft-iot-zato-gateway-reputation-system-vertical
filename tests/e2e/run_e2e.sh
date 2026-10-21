#!/usr/bin/env bash
set -euo pipefail

IMAGE="${IMAGE:-silviozv/soft-iot-gateway-zato-vertical:1.0.0}"
MIN_REQUESTS="${MIN_REQUESTS:-10}"

mapfile -t IDS < <(docker ps -q --filter "ancestor=${IMAGE}")
[ "${#IDS[@]}" -gt 0 ] || { echo "Nenhum container da imagem ${IMAGE} está rodando."; exit 1; }

URLS=()
for id in "${IDS[@]}"; do
  ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$id")
  name=$(docker inspect -f '{{.Name}}' "$id" | tr -d '/')
  echo "gateway: $name -> $ip"
  URLS+=("http://${ip}:11223")
done
GATEWAYS=$(IFS=,; echo "${URLS[*]}")

# rede do primeiro gateway (normalmente "bridge")
NETWORK=$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' "${IDS[0]}")

# opcional: MALICIOUS_CONTAINER=nome-do-container-malicioso
EXTRA_ENV=()
if [ -n "${MALICIOUS_CONTAINER:-}" ]; then
  mip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$MALICIOUS_CONTAINER")
  EXTRA_ENV+=(-e "EXPECT_LOWEST_REPUTATION=http://${mip}:11223")
fi
[ "${EXPECT_ALL_HONEST:-0}" = "1" ] && EXTRA_ENV+=(-e EXPECT_ALL_HONEST=1)

docker run --rm --network "$NETWORK" \
  -v "$(pwd)":/work -w /work \
  -e GATEWAYS="$GATEWAYS" -e MIN_REQUESTS="$MIN_REQUESTS" \
  -e E2E_TIMEOUT="${E2E_TIMEOUT:-900}" \
  "${EXTRA_ENV[@]}" \
  python:3.11-slim \
  bash -c "pip install -q pytest requests && pytest tests/e2e -v -s -p no:cacheprovider"