# Soft-IoT Gateway: Sistema de Reputação Distribuída (Zato)

Gateway IoT baseado no **Zato ESB**, integrado ao ecossistema **Soft-IoT** e a um ledger distribuído (**IOTA / Tangle, via nó Hornet**). Cada instância de gateway:

- coleta dados de dispositivos por MQTT (protocolo **TATU**) e os expõe por APIs REST;
- publica e consulta **mensagens** indexadas na Tangle;
- executa um protocolo de **descoberta de serviços**, **seleção por reputação** e **avaliação entre pares**;
- aplica um modelo de **confiança vertical**: registra seus dispositivos na Tangle, avalia os dados que recebe deles e escolhe a dupla **gateway/dispositivo** de maior reputação conjunta;
- pode simular nós honestos, maliciosos, egoístas e perturbadores.

---

## Sumário

1. [Glossário](#glossário)
2. [Visão geral](#visão-geral)
3. [Versão da DLT](#versão-da-dlt)
4. [Arquitetura](#arquitetura)
5. [Dispositivos IoT e protocolo TATU](#dispositivos-iot-e-protocolo-tatu)
6. [Canais de comunicação com a DLT](#canais-de-comunicação-com-a-dlt)
7. [Formato das mensagens na DLT](#formato-das-mensagens-na-dlt)
8. [Tipos de nó (condutas)](#tipos-de-nó-condutas)
9. [Módulos de serviço](#módulos-de-serviço-implsrcservices)
10. [Fluxo de descoberta de serviços e reputação](#fluxo-de-descoberta-de-serviços-e-reputação)
11. [Cálculo de reputação](#cálculo-de-reputação)
12. [Agendamento e APIs REST](#agendamento-e-apis-rest)
13. [Execução local (uma máquina)](#execução-local-uma-máquina)
14. [Execução experimental distribuída (Fogbed)](#execução-experimental-distribuída-fogbed)
15. [Resultados experimentais](#resultados-experimentais)
16. [Variáveis de ambiente](#variáveis-de-ambiente)
17. [Empacotamento (Dockerfile)](#empacotamento-dockerfile)
18. [Como construir e executar](#como-construir-e-executar)
19. [Observações e limitações](#observações-e-limitações)
20. [Trabalhos futuros](#trabalhos-futuros)
21. [Estrutura de diretórios](#estrutura-de-diretórios)
22. [Referência rápida de desenvolvimento](#referência-rápida-de-desenvolvimento)
23. [Referências](#referências)

---

## Glossário

| Termo | Significado neste projeto |
|---|---|
| **Gateway** | Instância deste projeto (container Zato). É o nó de névoa do sistema. Na parte de reputação, também é chamado de "nó" (`node_id`, `select_best_node`). |
| **Nó Hornet** | Software de nó da rede IOTA. Mantém a Tangle e expõe a API REST e a API de eventos MQTT. Não confundir com o "nó" (gateway) da reputação. |
| **Tangle** | Estrutura distribuída (DAG) que armazena as mensagens. |
| **Mensagem (*message*)** | Unidade de dados da Tangle na IOTA 1.5 (Chrysalis). Aqui carrega um payload de **indexação** (`index` + `data`). |
| **Índice (*index*)** | Rótulo da mensagem na Tangle. Permite filtrar e consultar mensagens (ex.: `REP_HAS_SVC`). |
| **Tangle Hornet API** | API REST em Go que encapsula a API REST do nó Hornet e faz a prova de trabalho (PoW). Vem **embutida na imagem do gateway** e roda dentro do mesmo container, em `127.0.0.1:3000`. |
| **DLT MQTT Manager** | Componente do gateway (singleton em Python) que se conecta ao **broker MQTT do nó Hornet**, assina os índices de interesse e despacha as mensagens para serviços Zato. Substitui a antiga ponte ZMQ. |
| **TATU** | Protocolo de aplicação sobre MQTT usado entre dispositivos e gateway (registro, `GET`, `FLOW`). |
| **Worker** | Máquina física de um experimento Fogbed. |
| **Provedor / Solicitante** | Gateway que oferece um serviço / gateway que o requisita. Todo gateway assume os dois papéis ao longo do tempo. |
| **Reputação** | Média das avaliações registradas sobre um nó; representa a opinião global do sistema. |
| **Credibilidade** | Medida da veracidade da opinião de um avaliador, obtida pela **confiabilidade** (comparação com a opinião global) e pela **consistência** (comparação com a opinião anterior do próprio avaliador). |

---

## Visão geral

O sistema modela uma rede de **gateways** que:

1. **Armazenam localmente** as leituras de sensores conectados (TATU/MQTT).
2. **Anunciam e descobrem serviços** (tipos de sensor) por meio de mensagens indexadas na Tangle.
3. **Recebem as propostas** de outros gateways em tempo real, assinando diretamente o broker MQTT do nó Hornet.
4. **Calculam a reputação** dos candidatos a partir do histórico de avaliações gravado na Tangle, filtrando os avaliadores por **credibilidade** (K-Means e métricas de confiabilidade e consistência).
5. **Consomem os dados** do gateway escolhido por HTTP direto (REST Zato, porta `11223`) e **publicam avaliações** na Tangle.

A orquestração periódica (inicialização, agregação, limpeza e ciclo de reputação) é feita pelo **Enmasse**, por meio de `config/enmasse/enmasse.yaml`, que registra os serviços REST e um **external_scheduler** que dispara URLs locais na porta `11223`.

O cálculo de reputação e credibilidade segue as definições de Santos [[12]](#referências), com adaptações (prior de reputação com peso configurável e ajuste dos parâmetros ao ambiente emulado).

---

## Versão da DLT

| Item | Valor |
|---|---|
| Rede | IOTA 1.5 (**Chrysalis**), rede **privada** montada pelo script do Fogbed |
| Nó | **Hornet 1.2.x** |
| Tangle Hornet API | [`larsid/tangle-hornet-api`](https://github.com/larsid/tangle-hornet-api), release **v1.0.1**, compilada no `Dockerfile` |
| Cliente de eventos do gateway | `paho-mqtt`, com decodificação própria do formato binário da mensagem |
| Unidade de dados | *Message*, com payload de indexação (`index` + `data`) |
| Portas do Hornet | REST: `14265`; MQTT (eventos): `1883` |
| `minPoWScore` | **100** na Tangle local (`fogbed_tangle_local.py`); o padrão do `fogledgerIota` é 2000 |

A terminologia deste projeto (`message`, `messageId`, `/message`) segue a Chrysalis. Em versões posteriores (Stardust) esses termos mudam para *block* e *tagged data*.

---

## Arquitetura

### Organização mínima

A instância mínima da infraestrutura tem um nó Hornet, um gateway (com a Tangle Hornet API embutida) e um dispositivo.

<p align="center">
  <img src="docs/images/architecture.png" width="350" alt="Organização mínima da arquitetura do sistema">
  <br><em>Figura 1. Organização mínima da arquitetura do sistema, abstraindo a escalabilidade. A figura é da versão anterior, com a Tangle Hornet API e a ponte ZMQ em containers separados.</em>
</p>

### Visão por camadas (texto)

```text
                         ┌──────────── container do gateway ────────────┐
Dispositivos ─MQTT/TATU─►│ Zato ──HTTP 127.0.0.1:3000──► Tangle Hornet API │──REST :14265──► Nó Hornet ◄──P2P──► Outros nós
                         │  ▲                                             │                    │
                         └──┼─────────────────────────────────────────────┘                    │
                            └──────────── MQTT :1883 (messages/indexation/<hex do índice>) ────┘
```

Cada gateway fala com o nó Hornet por dois caminhos: **escrita e consulta** pela Tangle Hornet API embutida, e **eventos em tempo real** assinando o broker MQTT do próprio Hornet. Não há containers intermediários.

### Componentes do gateway

- **Armazenamento local:** broker MQTT interno e SQLite (leituras dos dispositivos).
- **Mapeamento e API REST:** `devices.json`, comandos TATU e endpoints de dados.
- **Cliente DLT:** serviços Zato que chamam a Tangle Hornet API embutida por HTTP.
- **DLT MQTT Manager:** cliente MQTT do broker do Hornet, assinando o índice `REP_HAS_SVC`.
- **Tarefas de reputação e tipos de nó:** descoberta, seleção, avaliação e credibilidade.
- **Confiança dos dispositivos:** registro dos dispositivos na Tangle, avaliação dos seus dados e reputação do dispositivo.
- **Gerenciadores de estado:** identidade (`IDManager`), estado e reputação local (`GatewayStateManager`) e conduta (`NodeTypeManager`).

### Topologia em experimentos distribuídos

- Há **um nó Hornet por worker**; o coordenador e o spammer ficam no worker 0.
- Cada gateway tem a **sua própria** Tangle Hornet API e a **sua própria** conexão MQTT, ambas apontando para o nó Hornet do seu worker (`TANGLE_NODE_URL` e `Zato_DLT_MQTT_IP`).
- Gateways de workers diferentes comunicam-se por dois meios: **indiretamente**, pela Tangle (P2P entre os nós Hornet), na descoberta; e **diretamente**, por HTTP no IP do provedor, no consumo do serviço.

---

## Dispositivos IoT e protocolo TATU

A camada de percepção é formada por dispositivos virtuais em Python que operam como clientes MQTT e emulam sensores. Cada dispositivo se conecta ao **broker MQTT local do gateway**, o que permite vários dispositivos por gateway.

<p align="center">
  <img src="docs/images/sequence_diagram.png" width="420" alt="Diagrama de sequência da comunicação do dispositivo com o gateway">
  <br><em>Figura 2. Diagrama de sequência da comunicação do dispositivo com o gateway.</em>
</p>

1. O dispositivo conecta ao broker e assina seu tópico exclusivo de comandos (`dev/<ID do dispositivo>`).
2. Publica o **registro de capacidades** em `dev/CONNECTIONS`: identificação, coordenadas aleatórias (latitude e longitude) e declaração dos sensores instalados, com os tempos base de coleta e publicação. Isso ocorre **uma única vez, sem reenvio**.
3. Passa a escutar comandos. O protocolo suporta duas operações, com respostas em JSON publicadas em `dev/<ID do dispositivo>/RES`:
   - **`GET`**: leitura instantânea do sensor solicitado;
   - **`FLOW`**: envio contínuo, com intervalo de coleta e intervalo de publicação.

Os valores gerados pelos sensores virtuais são **flutuantes aleatórios** (0 a 100), pois a magnitude dos dados não é relevante para o sistema de reputação. O que importa é que o dado seja válido: um valor fora dessa faixa (o dispositivo malicioso envia `-1.0`) é considerado inválido.

---

## Canais de comunicação com a DLT

O gateway usa dois canais independentes, que só se encontram na Tangle.

| | Canal REST | Canal MQTT |
|---|---|---|
| Caminho | Zato → Tangle Hornet API (no mesmo container) → REST do Hornet | Broker MQTT do Hornet → Zato (`DLTMQTTManager`) |
| Papel | Escrever e consultar | Receber notificações em tempo real |
| Iniciativa | Do gateway (requisição/resposta) | Do nó (o gateway só escuta) |
| Sentido | Bidirecional | Somente Tangle → gateway |
| Modelo | *Pull* | *Push* |

### Canal REST: Tangle Hornet API embutida

O `entrypoint.sh` da imagem gera `/etc/tangle-hornet.conf` a partir de `API_PORT`, `TANGLE_NODE_URL` e `TANGLE_NODE_PORT`, inicia a API em segundo plano (log em `/opt/api-tangle/api.log`) e só depois inicia o Zato.

Serviços Zato do gateway (`requests`, timeout de 10 s) chamam a API definida por `Zato_TANGLE_API_IP` e `Zato_TANGLE_API_PORT` (padrão `127.0.0.1:3000`). A API faz a prova de trabalho localmente, com a dificuldade (`minPoWScore`) informada pelo nó. Por isso a dificuldade da Tangle pesa diretamente na CPU dos gateways.

| Serviço Zato | Chamada | Uso |
|---|---|---|
| `soft-iot.dlt.client.api.write` | `POST /message` `{index, data}` | Gravar uma mensagem indexada |
| `soft-iot.dlt.client.api.read_index` | `GET /message/{index}` | Ler as mensagens de um índice |
| `soft-iot.dlt.client.api.read_id` | `GET /message/messageId/{id}` | Ler uma mensagem pelo ID |

A API também oferece `GET /nodeInfo`, `GET /nodeInfo/all` e `GET /message/{index}/{maxMessages}` (até N mensagens da última hora), que o gateway não utiliza nos serviços atuais.

### Canal MQTT: eventos do Hornet

O `DLTMQTTManager` (`soft_iot_dlt_mqtt.py`) é um cliente `paho-mqtt` iniciado pelo serviço `soft-iot.dlt.mqtt.start`:

1. Conecta em `Zato_DLT_MQTT_IP:Zato_DLT_MQTT_PORT` (padrão `127.0.0.1:1883`) com `client_id` único (`SoftIoT_DLT_<uuid4>`). Dois clientes com o mesmo `client_id` se derrubariam um ao outro no broker.
2. Assina, com QoS 2, o tópico `messages/indexation/<índice em hexadecimal>` de cada índice em `Zato_DLT_MQTT_INDEXES` (padrão `REP_HAS_SVC`). As assinaturas são refeitas a cada reconexão (espera de 1 a 10 s).
3. Decodifica o payload binário da mensagem Chrysalis (`parse_indexation_message`) e extrai `index` e `data`.
4. Despacha conforme o `type`: `REP_SVC_REQ` → `soft-iot.dlt.handler.request`; `REP_SVC_RES` → `soft-iot.dlt.handler.response`, ambos com `invoke_async`.

Leiaute do payload binário (little-endian), como o Hornet publica:

```text
networkId u64 | nº de pais u8 | pais (32 bytes cada) | tamanho do payload u32 |
tipo do payload u32 (= 2, indexação) | tamanho do índice u16 | índice | tamanho dos dados u32 | dados | nonce u64
```

### O que passa por cada canal

| Índice | Conteúdo | Canal |
|---|---|---|
| `REP_HAS_SVC` | `REP_SVC_REQ` e `REP_SVC_RES` | **MQTT** (tempo real) e REST |
| `<gateway_id>` | `REP_EVALUATION` | **Somente REST** (`read_index`) |
| `CRED_<evaluator_id>` | `CRED_UPDATE` | **Somente REST** (`read_index`) |
| `DEVS_<gateway_id>` | `DEVICE_REGISTER` | **Somente REST** (`read_index`) |
| `DEV_<device_id>` | `REP_DEVICE_EVALUATION` | **Somente REST** (`read_index`) |

Escritas sempre usam o REST. O MQTT não permite escrever nem consultar o histórico.

### Observações

- O MQTT do Hornet **não replica nada**. A propagação entre nós é feita pelo P2P do Hornet. O broker apenas entrega ao gateway, em tempo real, os eventos do nó ao qual ele está ligado.
- A sessão MQTT **não guarda mensagens** para clientes desconectados: um gateway fora do ar perde o que foi publicado nesse intervalo.
- Um gateway **recebe de volta as próprias mensagens** que escreveu. O tratador de requisições ignora as suas (compara `source` com o próprio id).
- A antiga integração por ZMQ (`soft_iot_zmq_server.py`) continua no código, mas seus canais estão comentados no `enmasse.yaml` e ela não é mais iniciada.

---

## Formato das mensagens na DLT

O envelope tem dois níveis. A mensagem da Tangle carrega um payload de indexação (`index` + `data`), e o campo `data` é uma **string JSON** da aplicação, com o campo `type`. O DLT MQTT Manager decodifica o binário e depois faz o parse do JSON.

Exemplo, já decodificado, do que o gateway recebe pelo MQTT (tópico `messages/indexation/5245505f4841535f535643`, que é `REP_HAS_SVC` em hexadecimal):

```json
{
  "index": "REP_HAS_SVC",
  "data": "{\"type\":\"REP_SVC_REQ\",\"idRequest\":\"...\",\"source\":\"<gateway_id>\",\"group\":\"cloud/c1\",\"requestedService\":\"Thermometer\"}"
}
```

### Tipos de mensagem (`type`)

| `type` | Índice | Descrição |
|---|---|---|
| `REP_SVC_REQ` | `REP_HAS_SVC` | Pedido de descoberta: serviço desejado (`requestedService`), `idRequest`, `source`, `group` |
| `REP_SVC_RES` | `REP_HAS_SVC` | Oferta: `target`, `idRequest`, `source`, `ipSource`, `group`, `requestedService`, `hasService`. A lista `services` vai vazia: o requisitor consulta os dispositivos na Tangle |
| `REP_EVALUATION` | `<gateway_id>` do avaliado | Avaliação do provedor: `value`, `serviceEvaluation`, `nodeCredibility`, `source`, `target` |
| `CRED_UPDATE` | `CRED_<evaluator_id>` | Nova credibilidade do avaliador |
| `DEVICE_REGISTER` | `DEVS_<gateway_id>` | Registro de um dispositivo pelo seu gateway: `gateway_id`, `device_id`, `latitude`, `longitude`, `sensors` (`id`, `type`) |
| `REP_DEVICE_EVALUATION` | `DEV_<device_id>` | Notas (0 ou 1) dadas pelo gateway aos dados recebidos do dispositivo: `source`, `target`, `sensor_ids`, `serviceEvaluations` (uma nota por leitura recebida no intervalo) |

---

## Tipos de nó (condutas)

Uma "taxa de honestidade" (0 a 100) controla os comportamentos nocivos: a cada ação, um valor aleatório é sorteado e comparado à taxa, definindo se o nó age de forma correta ou subversiva naquela interação.

### Dispositivo

Definido pela variável `SUCCESS_RATE` do dispositivo virtual. O dispositivo só atua como **provedor** (envia leituras ao seu gateway).

| Conduta | Comportamento |
|---|---|
| Honesto (`SUCCESS_RATE=100`) | Envia apenas dados válidos (0 a 100). |
| Malicioso (`SUCCESS_RATE<100`) | A cada leitura, pela taxa, envia um dado válido ou inválido (`-1.0`). |

### Gateway

Definido por `Zato_NODE_TYPE` e `Zato_HONESTY_RATE` (`NodeTypeManager`). A conduta é sorteada separadamente em cada papel.

| Tipo | Valor | Como provedor (requisição de outro gateway) | Como avaliador (dado do dispositivo ou de outro gateway) |
|---|---|---|---|
| Honesto | `1` | Confirma que tem o serviço e repassa o dado real. | Nota 1 para dado válido, 0 para inválido. |
| Malicioso | `2` | Confirma que tem o serviço. Na requisição direta, pela taxa, repassa o dado real ou injeta um dado inválido (`-1.0`). | Pela taxa, **inverte** a nota correta. |
| Egoísta | `3` | **Não responde** ao broadcast; por isso não recebe requisições diretas. | Consome o dado, mas **não avalia**: não grava notas nem atualiza a credibilidade na Tangle. |
| Perturbador | `4` | Honesto até a reputação própria passar de 0,9; depois, malicioso (permanente). | Honesto até o limiar; depois, malicioso. |

---

## Módulos de serviço (`impl/src/services`)

### Identidade e estado local

| Arquivo | Serviço Zato | Função |
|---|---|---|
| `soft_iot_id_manager.py` | `soft-iot.id.manager` | Singleton: UUID do gateway, IP e grupo (`Zato_GROUP`). O UUID é gravado em `/tmp/gateway_id` (criação atômica), para que todas as cópias do módulo carregadas pelo Zato usem o mesmo ID. O IP vem de `Zato_GATEWAY_REAL_IP` ou, se ela não existir, é descoberto pelo hostname do container. |
| `soft_iot_gateway_state.py` | (DAO usado pelas tarefas) | SQLite em `/tmp/gateway_state.db`: reputação local, flag de mudança de comportamento e máquina de estados das requisições (`WAITING_RESPONSES` → ... → `FINISHED`). |

### Armazenamento IoT e API de dados

| Arquivo | Serviços | Função |
|---|---|---|
| `soft_iot_storage.py` | `soft-iot.storage.service` | Inicia o broker MQTT interno, persiste leituras em `/opt/zato/env/soft_iot_data.db` e cadastra dispositivos automaticamente. No handshake, despacha o registro do dispositivo na Tangle; a cada dado recebido, despacha a avaliação do dispositivo. |
| `soft_iot_data_exposure.py` | `soft-iot.api.*`, `soft-iot.api.provide-service-data` | Endpoints REST de dados. `provide-service-data` é o retorno de serviço para outro gateway e aplica a conduta de provedor. |
| `soft_iot_aggregation.py` | `soft-iot.aggregation.service` | Agrega leituras brutas em janelas (`Zato_AGGREGATION_WINDOW_MINUTES`). |
| `soft_iot_cleanup.py` | `soft-iot.cleanup.service` | Remove dados antigos (`Zato_DATA_RETENTION_SECONDS`). |
| `soft_iot_api.py` | `soft-iot.api.*` | Endpoints REST: último dado, histórico e agregados. |

### Mapeamento de dispositivos (TATU)

| Arquivo | Serviços | Função |
|---|---|---|
| `soft_iot_mapping.py` | `soft-iot.mapping.*` | Lê `devices.json`, lista dispositivos e envia comandos `GET`/`SET`/`FLOW` por MQTT (TATU). |

### Cliente DLT

| Arquivo | Serviços | Função |
|---|---|---|
| `soft_iot_dlt_client_tangle.py` | `soft-iot.dlt.client.api.write`, `read_index`, `read_id` | HTTP para a Tangle Hornet API embutida: gravar mensagens indexadas e ler histórico. |

### Eventos da DLT em tempo real (MQTT)

| Arquivo | Serviços | Função |
|---|---|---|
| `soft_iot_dlt_mqtt.py` | `soft-iot.dlt.mqtt.start`, `soft-iot.dlt.handler.request`, `soft-iot.dlt.handler.response` | `DLTMQTTManager`: cliente do broker MQTT do Hornet que decodifica as mensagens e as despacha. O tratador de **requisição** procura em `devices.json` os sensores cujo `type` seja igual ao serviço pedido e que já tenham dado armazenado, e responde com `REP_SVC_RES` (o egoísta não responde). O tratador de **resposta** só aceita se o gateway for o `target` e se a requisição estiver em `WAITING_RESPONSES`. |
| `soft_iot_zmq_server.py` | `soft-iot.zmq.*` | Versão anterior, por ponte ZMQ. Mantida no código, mas **desativada** (canais comentados no `enmasse.yaml`). |

A Tangle continua sendo a fonte persistida de verdade. O MQTT apenas evita *polling* para saber das novas mensagens.

### Confiança dos dispositivos (modelo vertical)

| Arquivo | Serviços | Função |
|---|---|---|
| `soft_iot_device_trust.py` | `soft-iot.device.register` | Grava `DEVICE_REGISTER` em `DEVS_<gateway_id>` quando o dispositivo inicia a comunicação. |
| | `soft-iot.device.evaluation` | Avalia cada leitura recebida do dispositivo (nota 0 ou 1, conforme a conduta de avaliador, **sem credibilidade**) e grava `REP_DEVICE_EVALUATION` em `DEV_<device_id>`. |
| | `soft-iot.dlt.gateway.devices` | Lê `DEVS_<gateway_id>` e devolve os pares `(device_id, sensor_id)` com o serviço pedido. |
| | `soft-iot.reputation.device` | Reputação do dispositivo: média das notas do seu gateway, com o mesmo prior de 0,5. |

### Comportamento simulado dos nós

| Arquivo | Serviço | Função |
|---|---|---|
| `soft_iot_node_type.py` | `soft-iot.node.evaluation` | `NodeTypeManager`: define a conduta e distorce as avaliações quando malicioso. |

### Reputação e credibilidade

| Arquivo | Serviços | Função |
|---|---|---|
| `soft_iot_kmeans.py` | `soft-iot.kmeans.credibility` | K-Means (k=4) sobre as credibilidades dos avaliadores; seleciona o cluster de maior valor (avaliadores "confiáveis"). |
| `soft_iot_reputation_node.py` | `soft-iot.reputation.orchestrator`, `soft-iot.reputation.credibility.manager` | Orquestrador: lê `REP_EVALUATION`, filtra por K-Means e calcula a média ponderada com prior neutro. Credibility manager: atualiza a credibilidade por *reliability* (vs. consenso) e *consistency* (vs. avaliação anterior). |
| `soft_iot_service_handler.py` | `soft-iot.reputation.task.*`, `soft-iot.gateway.state.get` | Pipeline de tarefas (ver fluxo abaixo). |

---

## Fluxo de descoberta de serviços e reputação

Ciclo disparado pelo scheduler (`check-nodes-services`, a cada 20 s). A propagação entre gateways passa pelo P2P do Hornet. O broker MQTT de cada nó Hornet entrega os eventos aos gateways ligados a ele.

### Fluxo local (dispositivo → gateway)

1. O dispositivo envia o handshake com sua identificação e sensores.
2. O gateway grava o dispositivo em `devices.json` e o **registra na Tangle** (`DEVICE_REGISTER`).
3. O gateway envia os comandos `FLOW`.
4. A cada mensagem de dados, o gateway armazena as leituras e as guarda para avaliação. Uma thread dedicada, a cada `Zato_DEVICE_EVAL_FLUSH_SECONDS` (padrão 20 s), **avalia cada leitura** conforme a conduta de avaliador e grava um único `REP_DEVICE_EVALUATION` por dispositivo. Assim, a escrita na Tangle não trava a thread do MQTT.

### Tarefas (`soft_iot_service_handler.py`)

1. **`check_nodes_services`**: evita concorrência (timeout de 120 s). Cria a requisição local, publica `REP_SVC_REQ` em `REP_HAS_SVC` e dispara `wait_nodes_responses` de forma assíncrona, levando o serviço pedido.
2. **Gateways receptores** (`dlt.handler.request`, via MQTT): verificam em `devices.json` se algum dispositivo tem o serviço **e já tem dado armazenado** para esse sensor, e respondem apenas que o possuem (o egoísta não responde).
3. **`wait_nodes_responses`**: aguarda a janela de coleta. Se a requisição ainda estiver em `WAITING_RESPONSES`, passa para `CHOSING_BETTER_NODE` e chama `select_best_node`.
4. **`select_best_node`**: para cada gateway que respondeu, consulta na Tangle os dispositivos com o serviço, calcula a reputação do gateway e de cada dispositivo e forma a **reputação conjunta** `0,5 × gateway + 0,5 × dispositivo`. Escolhe a dupla de maior valor.
5. **`request_node_service`**: REST direto em `GET /soft-iot/provider/devices/{device_id}/sensors/{sensor_id}/data/latest` no IP do gateway escolhido (o provedor aplica sua conduta). Nota bruta: 1 se o dado for válido, 0 se for inválido ou se o consumo falhar. Aplica a conduta de avaliador, grava a avaliação **somente do gateway** e atualiza a credibilidade (exceto o egoísta).
6. **`calculate_reputation`**: recalcula a **própria** reputação do gateway e a persiste em `GatewayStateManager`.
7. **`change_disturbing_behavior`**: para o tipo 4, ativa a fase de perturbação quando a reputação local ultrapassa o limiar. É uma tarefa **recorrente (a cada 20 s)** no scheduler.

Status finais de uma requisição: `FINISHED` (dado válido), `FINISHED_INVALID_DATA` (dado inválido), `FAILED_DATA_CONSUMPTION` (falha no HTTP), `FAILED` (sem respostas), `FAILED_NO_SERVICES` (nenhuma dupla gateway/dispositivo encontrada na Tangle) e `TIMEOUT`.

A tabela `requests` também registra `requested_service`, `id_device`, `id_sensor`, `reputation_device`, `reputation_joint` e `received_data_value`.

---

## Cálculo de reputação

Para um gateway alvo `node_id`:

1. Ler, por `read_index`, as mensagens do índice `node_id` com `type == REP_EVALUATION`.
2. Extrair os pares `(value, nodeCredibility)` de cada avaliação.
3. Executar o K-Means (k=4) nas credibilidades e manter as avaliações cujo avaliador pertence ao cluster de maior credibilidade.
4. Reputação final: média das `value` dos avaliadores confiáveis, combinada com um **prior fixo de 0,5** de peso `Zato_REP_PRIOR_WEIGHT` (padrão 5). O prior evita que as primeiras avaliações causem mudanças bruscas e afastem precocemente as reputações dos nós. O resultado fica no intervalo **[0, 1]**.
5. Sem histórico: reputação neutra de **0,5** (metade do intervalo). Todos os gateways começam assim.

A credibilidade de cada avaliador evolui a cada interação, por meio do `CredibilityManager`, e é persistida em `CRED_<evaluator_id>`:

- **Confiabilidade (*reliability*)**: compara a avaliação do nó com a opinião global (consenso). Limiar: `Zato_RELIABILITY_THRESHOLD`.
- **Consistência (*consistency*)**: compara a avaliação atual com a anterior do mesmo avaliador. Limiar: `Zato_CONSISTENCY_THRESHOLD`.

Nos experimentos, os dois limiares foram fixados em **0,5**.

### Reputação do dispositivo

Lida de `DEV_<device_id>`, considerando apenas as notas do gateway dono do dispositivo. É a média das notas (0 ou 1), com o mesmo prior de 0,5 e peso `Zato_REP_PRIOR_WEIGHT`. Não usa credibilidade nem K-Means. Sem histórico, vale 0,5.

### Reputação conjunta

`reputação conjunta = 0,5 × reputação do gateway + 0,5 × reputação do dispositivo`. É usada apenas para escolher a dupla. A credibilidade do avaliador continua sendo comparada com a reputação do **gateway**.

---

## Agendamento e APIs REST

Configurados em `config/enmasse/enmasse.yaml`.

### Canal REST do gateway

Expõe os serviços sob `/soft-iot/...`, na porta padrão do Zato (**11223**). Agrupa:

- armazenamento local (start, agregação, limpeza, leituras de API);
- mapeamento de dispositivos e coleta TATU;
- cliente DLT e eventos MQTT da DLT (`/soft-iot/dlt/mqtt/...`);
- confiança dos dispositivos (`/soft-iot/device/...`, `/soft-iot/dlt/gateway/devices`, `/soft-iot/reputation/device`);
- K-Means, ID manager e avaliação de nó;
- tarefas de reputação e depuração de estado (`/soft-iot/reputation/task/get-data`).

Entre gateways, o endpoint usado no consumo do serviço é `GET /soft-iot/provider/devices/{device_id}/sensors/{sensor_id}/data/latest`, que aplica a conduta de provedor. O endpoint `GET /soft-iot/devices/{device_id}/sensors/{sensor_id}/data/latest` continua disponível para leitura local, sem conduta.

### External scheduler

Jobs HTTP para `http://localhost:11223/...` (independentes do scheduler interno do Zato, hoje em grande parte comentado):

| Job | Tipo | Intervalo | Propósito |
|---|---|---|---|
| ID manager | Execução única | +5 s | Inicializa a identidade |
| Local storage start | Execução única | +10 s | Inicia MQTT e SQLite |
| Conexão MQTT com o Hornet | Execução única | +15 s | Inicia o `DLTMQTTManager` (assina `REP_HAS_SVC`) |
| check-nodes-services | Recorrente | 20 s | Novo ciclo de descoberta |
| calculate-reputation | Recorrente | 20 s | Atualiza a reputação própria |
| change-disturbing-behavior | Recorrente | 20 s | Verifica se o nó perturbador deve mudar de conduta |
| Agregação | Recorrente | 5 min | Dispara a verificação e agregação de dados |
| Limpeza | Recorrente | 20 min | Remove dados expirados (deve ser maior que a janela de agregação) |

Ajuste intervalos e `initial_delay` ao tempo de boot dos containers.

---

## Execução local (uma máquina)

É o modo usado atualmente nos testes. São dois scripts, executados em terminais separados.

### 1. Tangle: `impl/scripts/fogbed_tangle_local.py`

Sobe com o Fogbed uma Tangle privada com 1 nó Hornet (`node0`), 1 coordenador (milestone a cada 60 s) e 1 spammer. A API **não** é criada aqui, porque vem dentro de cada gateway.

Entre a criação (`IotaBasic`) e a inicialização (`start_network`), `patch_hornet_templates()` ajusta os templates em `/tmp/iota/$USER/config/`:

| Ajuste | Valor | Motivo |
|---|---|---|
| `protocol.minPoWScore` | 2000 → **100** | Cada gateway faz o PoW localmente. Com 2000 e 4 gateways, a CPU satura e as escritas passam de 10 s; com 100, o PoW fica perto de zero. |
| `restAPI.limits.maxResults` | 1000 → 50000 | O histórico de avaliações de um nó é lido inteiro. |
| `snapshots.depth` | 50 → 500 | Margem para rodadas longas. |
| Spammer | `autostart` desligado | Evita PoW contínuo. |
| Plugins `Autopeering`, `P2PDiscovery`, `VersionCheck` | desligados | Rede privada com peering fixo. O MQTT continua ligado. |
| `db.autoRevalidation` | ligado | Recupera o banco se o processo morrer sem fechar. |
| `tangle.milestoneTimeout` | 120 s → 300 s | Tolerância a atraso do coordenador. |
| Pruning | `maxMilestonesToKeep` 5000, tamanho 2 GB | Disco e memória em execuções longas. |

Portas no host: `1881` (dashboard), `1894` (MQTT) e `14265` (REST). No fim, o script imprime o IP do `node0`: use-o como `HORNET_IP` no próximo script. Para encerrar, digite `p`.

### 2. Gateways e dispositivos: `impl/scripts/exec_architecture_local_continuous.py`

Executa N rodadas de X minutos e coleta os dados de cada uma. A Tangle precisa estar no ar.

- **Configuração** (topo do script): `HORNET_IP`, a lista `GATEWAYS` (`node_type`, `honesty_rate` e, opcionalmente, `device_success_rate`), `DEVICES_PER_GATEWAY`, `DEVICE_SUCCESS_RATE`, `RUNS`, `DURATION_MINUTES` e as imagens (`IMG_GATEWAY`, `IMG_DEVICE`).
- **Gateways** (`gat<i>`): recebem as variáveis da API embutida (`API_PORT`, `TANGLE_NODE_URL`, `TANGLE_NODE_PORT`), do MQTT da DLT (`Zato_DLT_MQTT_IP`, `Zato_DLT_MQTT_PORT`, `Zato_DLT_MQTT_INDEXES`) e da conduta. Iniciam com `sleep 10` antes do `entrypoint.sh`. O IP não é fixado: o gateway descobre o próprio.
- **Dispositivos** (`dev_<i>_<z>`): `BROKER_IP=gat<i>`, `SUCCESS_RATE` e `sleep 120` antes de iniciar (o handshake é enviado uma única vez).
- **Saída**, em `impl/scripts/data_relatorio_final/run_<N>/`: `gat<i>.log` (`docker logs`), `gat<i>_requests.csv` (tabela de requisições), `dados_consolidados.json` (estado de todos os gateways, via `/soft-iot/reputation/task/get-data`) e `meta.json` (parâmetros da rodada).

```bash
python3 impl/scripts/fogbed_tangle_local.py                                   # terminal 1
python3 impl/scripts/exec_architecture_local_continuous.py --runs 5 --minutes 10   # terminal 2
```

---

## Execução experimental distribuída (Fogbed)

> **Atenção:** `fogbed_exec.py` ainda segue a arquitetura anterior (ponte ZMQ e Tangle Hornet API em containers separados) e não foi atualizado para a imagem com a API embutida e o MQTT direto. Para usá-lo, é preciso remover a ponte e a API e passar aos gateways as variáveis da seção [Variáveis de ambiente](#variáveis-de-ambiente).

Script: **`impl/scripts/fogbed_exec.py`**. Monta um `FogbedDistributedExperiment` em que **todos os componentes são containers Docker** na rede emulada `10.0.0.0/8`. O [Fogbed](https://larsid.github.io/fogbed/) permite distribuir os containers em várias máquinas físicas (workers).

1. **`criar_tangle`**: rede IOTA privada (`IotaBasic`), com um nó Hornet por worker. O README original cita coordenador e spammer.
2. **`patch_hornet_templates`**: altera `restAPI.limits.maxResults` (1000 → 50000) e `snapshots.depth` (50 → 500). 
3. **Instâncias virtuais**: uma por worker.
4. **Ponte ZMQ** (`silviozv/iota-zmq-bridge:1.0.0`): `MQTT_IP` do nó Hornet do worker; `INDEXES=REP_*`. Uma por worker.
5. **Tangle Hornet API** (`silviozv/tangle-hornet-api:1.0.1`): porta 3000, ligada ao nó do worker (porta 14265). Uma por worker.
6. **`criar_gateways`**: imagem `silviozv/soft-iot-gateway-zato:1.0.0`, distribuídos entre os workers de forma circular. Cada gateway recebe a API e a ponte do **seu** worker. Executa como `root`, em modo `privileged`, com IP fixo e a porta 11223 exposta.
7. **`criar_devices`**: containers `silviozv/python-iot-device:1.0.0`, conectados ao broker MQTT do seu gateway (`BROKER_IP`, `BROKER_PORT=1883`), com IP fixo. A interface de rede é configurada manualmente no comando de inicialização.
8. **Túneis** entre workers consecutivos, seguidos de `exp.start()` e `iota.start_network()`.

### Sincronização por tempo de espera

A ordem de subida é controlada por esperas fixas, e não por verificação de prontidão:

- gateway: `sleep 60`, para dar tempo à Tangle;
- dispositivo: `sleep 120`, para o Zato e o broker do gateway ficarem prontos. Isso importa porque o dispositivo virtual envia o handshake **uma única vez**, sem reenvio: se o broker não estiver de pé, ele não se registra.

### Parâmetros de simulação

No `__main__`, o exemplo padrão usa 6 gateways honestos, 0 maliciosos, 0 egoístas, 0 perturbadores e 1 dispositivo por gateway. Antes dos gateways, o script gera `config/auto-generated/env.ini` (senhas de API e `Zato_Project_Root`).

**Pré-requisitos:**

- a quantidade de gateways deve ser maior ou igual à de workers;
- os workers precisam ser adicionados a `worker_list`. No script, todas as linhas `exp.add_worker('larsid..')` estão **comentadas**. Com a lista vazia, `criar_tangle` falha.

---

## Resultados experimentais

> Os resultados abaixo são da **versão anterior** do sistema: sem o modelo vertical (a escolha usava só a reputação do gateway), com a ponte ZMQ e a Tangle Hornet API em containers separados. Os resultados do modelo vertical ainda serão gerados.

### Metodologia

- **Topologia mínima, em uma única máquina:** 1 nó Hornet, 1 Tangle Hornet API, 1 ponte ZMQ e **4 gateways**, cada um com 1 dispositivo.
- **Condutas:** 3 gateways (`gat0`, `gat1`, `gat2`) são honestos; o `gat3` assume a conduta avaliada em cada cenário.
- **Repetições:** cada cenário foi executado em **5 rodadas independentes**, cada uma até que cada gateway registrasse ~**200 requisições**. Os gráficos mostram a **reputação média** entre as 5 execuções.
- **Parâmetros:** limiares de confiabilidade e consistência = **0,5**.
- **Máquina de teste:** Acer Nitro AN515-51, Intel Core i5-7300HQ (4 núcleos, 2,50 GHz), 8 GB DDR4, SSD SATA de 1 TB.
- **Serviços dos dispositivos:** todos os gateways possuem todos os serviços possíveis, de modo que a checagem de disponibilidade do serviço é praticamente irrelevante nos testes.

| Cenário | Conduta do `gat3` | Resultado observado |
|---|---|---|
| 1 | Malicioso, 30% de honestidade (agressivo) | Reputação suprimida, estável em torno de 0,4 |
| 2 | Malicioso, 70% de honestidade (furtivo) | Estabiliza em faixa intermediária (~0,6), claramente abaixo dos honestos |
| 3 | Perturbador, 0% de honestidade | Sobe rapidamente até ~0,8 (perto da requisição 20) e declina depois da mudança de conduta |
| 4 | Egoísta | Sem impacto negativo nos demais; todos têm trajetória crescente |

### Cenário 1: malicioso agressivo (30% de honestidade)

Todos os gateways partem de 0,5. `gat0` e `gat2` convergem rapidamente para valores altos; `gat1` converge mais devagar, mesmo sendo honesto, o que é atribuído à atuação do nó malicioso. A reputação do `gat3` é rapidamente suprimida.

<p align="center">
  <img src="docs/images/reputacao-cenario1-malicioso-30.png" width="700" alt="Evolução da reputação média: malicioso com 30% de honestidade">
  <br><em>Figura 3. Evolução da reputação média dos gateways no cenário 1 (malicioso, 30% de honestidade).</em>
</p>

### Cenário 2: malicioso furtivo (70% de honestidade)

Com a taxa maior de honestidade, a reputação do nó malicioso converge para uma faixa mais alta que no cenário 1. `gat0` e `gat2` passam mais tempo em faixas de reputação menores que as do cenário 1, o que indica que o nó malicioso teve mais impacto negativo sobre a confiança nos nós honestos. Ainda assim, há margem clara de separação entre o `gat3` e os honestos.

<p align="center">
  <img src="docs/images/reputacao-cenario2-malicioso-70.png" width="700" alt="Evolução da reputação média: malicioso com 70% de honestidade">
  <br><em>Figura 4. Evolução da reputação média dos gateways no cenário 2 (malicioso, 70% de honestidade).</em>
</p>

### Cenário 3: perturbador (0% de honestidade)

O `gat3` acumula reputação rapidamente atuando como honesto. Ao atingir um valor alto (perto da requisição 20), muda de conduta e passa a agir de forma maliciosa em todas as interações; a partir daí sua reputação declina até o fim do experimento. Os gateways honestos sofrem oscilações, mas terminam com reputação maior que a do perturbador.

<p align="center">
  <img src="docs/images/reputacao-cenario3-perturbador.png" width="700" alt="Evolução da reputação média: perturbador com 0% de honestidade">
  <br><em>Figura 5. Evolução da reputação média dos gateways no cenário 3 (perturbador, 0% de honestidade).</em>
</p>

### Cenário 4: egoísta

Todos os gateways registram trajetória crescente e constante. O nó egoísta não degrada a reputação dos demais: ele apenas deixa de participar das avaliações.

<p align="center">
  <img src="docs/images/reputacao-cenario4-egoista.png" width="700" alt="Evolução da reputação média: egoísta">
  <br><em>Figura 6. Evolução da reputação média dos gateways no cenário 4 (egoísta).</em>
</p>

### Síntese

- O modelo isola nós altamente prejudiciais (cenário 1) e mantém separação mesmo diante de nós de comportamento malicioso mínimo (cenário 2).
- O sistema revoga a confiança de um nó antes lido como honesto quando seu comportamento malicioso é detectado (cenário 3).
- A reputação isolada é **insensível à não colaboração**: o egoísta mantém reputação alta enquanto não avalia ninguém (cenário 4). Detectar esse tipo de omissão exige outro mecanismo.

---

## Variáveis de ambiente

### Gateway

| Variável | Uso | Valor na execução local |
|---|---|---|
| `Zato_NODE_TYPE` | 1 honesto, 2 malicioso, 3 egoísta, 4 perturbador | por gateway |
| `Zato_HONESTY_RATE` | Probabilidade de conduta honesta (malicioso e perturbador na fase 2) | 100 (honesto e egoísta); configurável nos demais |
| `Zato_GATEWAY_REAL_IP` | IP anunciado nas respostas de serviço (`ipSource`). **Opcional:** sem ela, o gateway descobre o próprio IP. | não definida |
| `Zato_GROUP` | Grupo lógico na DLT | `cloud/c1` |
| `API_PORT` | Porta da Tangle Hornet API embutida (lida pelo `entrypoint.sh`) | `3000` |
| `TANGLE_NODE_URL` / `TANGLE_NODE_PORT` | Nó Hornet usado pela API embutida | `HORNET_IP` / `14265` |
| `Zato_TANGLE_API_IP` / `Zato_TANGLE_API_PORT` | Cliente DLT do Zato (padrão `127.0.0.1:3000`) | `127.0.0.1` / `3000` |
| `Zato_DLT_MQTT_IP` / `Zato_DLT_MQTT_PORT` | Broker MQTT do nó Hornet (padrão `127.0.0.1:1883`) | `HORNET_IP` / `1883` |
| `Zato_DLT_MQTT_INDEXES` | Índices assinados por MQTT, separados por vírgula | `REP_HAS_SVC` |
| `Zato_COLLECTION_TIME` / `Zato_PUBLISH_TIME` | Intervalos de coleta e publicação, em segundos, do ciclo MQTT do armazenamento local | `2` / `6` |
| `Zato_AGGREGATION_WINDOW_MINUTES` | Janela de agregação | `10` |
| `Zato_DATA_RETENTION_SECONDS` | Retenção antes da limpeza | `1200` |
| `Zato_REP_PRIOR_WEIGHT` | Peso do prior 0,5 no cálculo de reputação | padrão 5 |
| `Zato_DEVICE_EVAL_FLUSH_SECONDS` | Intervalo de envio das avaliações acumuladas de cada dispositivo | padrão 20 |
| `Zato_RELIABILITY_THRESHOLD` | Limiar de comparação da confiabilidade | 0,5 nos experimentos |
| `Zato_CONSISTENCY_THRESHOLD` | Limiar de comparação da consistência | 0,5 nos experimentos |
| `Zato_Dashboard_Password`, `ZATO_SSH_PASSWORD`, `Zato_IDE_Password` | Credenciais do Zato | `123456` (apenas experimento) |

### Dispositivo virtual

| Variável | Padrão | Descrição |
|---|---|---|
| `BROKER_IP` / `BROKER_PORT` | `127.0.0.1` / `1883` | Broker MQTT do gateway |
| `DEVICE_NAME` | UUID aleatório | Id do dispositivo (no Fogbed não é definido) |
| `SUCCESS_RATE` | `100` | Probabilidade (0 a 100) de cada leitura ser válida. Abaixo de 100, o dispositivo é malicioso. |

---

## Empacotamento (Dockerfile)

Build em **dois estágios**:

1. **`golang:1.20`**: clona [`larsid/tangle-hornet-api`](https://github.com/larsid/tangle-hornet-api) na tag `API_VERSION` (padrão `v1.0.1`) e compila o binário estático `api-tangle`.
2. **`rhianpablo11/esb-zato-soft-iot:v12`**: faz o **hot-deploy** do projeto Zato, copia o binário da API para `/usr/local/bin/api-tangle` e cria o `entrypoint.sh`.

| Destino no container | Origem |
|---|---|
| `/opt/hot-deploy/myproject/` | Projeto completo (código, serviços, scripts) |
| `/opt/hot-deploy/enmasse/enmasse.yaml` | `config/enmasse/enmasse.yaml` |
| `/opt/hot-deploy/enmasse/env.ini` | `config/auto-generated/env.ini` (gere antes do build) |
| `/opt/hot-deploy/python-reqs/requirements.txt` | Dependências Python adicionais (inclui `paho-mqtt`) |
| `/home/ubuntu/mapping_archives/devices_config/` | Arquivos de mapeamento de dispositivos (`impl/src/archives`) |
| `/usr/local/bin/api-tangle` | Binário da Tangle Hornet API (estágio 1) |
| `/usr/local/bin/entrypoint.sh` | Gera `/etc/tangle-hornet.conf`, inicia a API em segundo plano e executa o `start_wrapper.sh` do Zato |

Para usar outra versão da API: `docker build --build-arg API_VERSION=<tag> ...`.

Os serviços Python ficam em `impl/src/services/` e são carregados pelo runtime do Zato.

---

## Como construir e executar

1. **Construir a imagem do gateway**, na raiz do projeto:
   ```bash
   docker build -t silviozv/soft-iot-gateway-zato-vertical:1.0.0 .
   ```
   Reconstrua sempre que os serviços forem alterados: o container usa o código copiado no build. Para conferir se a imagem tem a versão nova, use `docker run --rm --entrypoint grep <imagem> -n "<trecho>" /opt/hot-deploy/myproject/impl/src/services/<arquivo>.py`.
2. **Imagens necessárias:** a do gateway, `silviozv/python-iot-device:1.0.1` e as imagens do Hornet usadas pelo `fogledgerIota`. A ponte ZMQ e a imagem separada da Tangle Hornet API não são mais necessárias.
3. **Preparar o ambiente:** instalar o Fogbed e o pacote `fogledgerIota`.
4. **Executar** conforme [Execução local](#execução-local-uma-máquina): primeiro a Tangle, depois as rodadas.
5. **Acompanhar:** `docker logs -f gat0`. Os loggers mais úteis são `zato.dlt.mqtt` (conexão e mensagens do Hornet) e `zato.id.manager` (ID e IP do gateway). O log da API embutida fica em `/opt/api-tangle/api.log`, dentro do container.

---

## Observações e limitações

- **Estado volátil:** o estado do gateway fica em `/tmp/gateway_state.db`, que se perde ao reiniciar o container.
- **Credenciais fixas:** as senhas (`123456`, `senha123`) servem apenas para o experimento. Não use em produção.
- **MQTT sem histórico:** um gateway que estiver fora do ar perde as mensagens `REP_HAS_SVC` publicadas nesse período.
- **PoW nos gateways:** como a API embutida faz o PoW localmente, a dificuldade da Tangle pesa na CPU de cada gateway. Mantenha `minPoWScore` baixo em execuções numa única máquina.
- **Ordem de carga do Zato:** o hot-deploy carrega os módulos em ordem alfabética, e um `import` de outro módulo `soft_iot_*` no topo do arquivo pode falhar se ele ainda não foi carregado. Por isso os serviços importam esses módulos **dentro das funções**. O Zato também pode carregar mais de uma cópia de um módulo; estado que precisa ser único no container (como o ID do gateway) deve ficar fora da memória do módulo.
- **Sincronização por tempo:** a ordem de subida depende de esperas fixas. Se o ambiente for lento, aumente-as.
- **Escolha sempre pela maior reputação conjunta:** sem nenhuma exploração, a escolha tende a se concentrar em poucas duplas. Gateways com conjunta baixa, como um malicioso que derruba a reputação do próprio dispositivo ao inverter as notas dele, quase não são escolhidos, e a conduta deles como provedor é pouco exercitada.
- **Valores dos sensores:** os dispositivos virtuais geram valores aleatórios (0 a 100) para qualquer tipo de sensor.
- **Amostra limitada nos testes:** os resultados vêm de 5 execuções por cenário, em uma única máquina e com 4 gateways. A escalabilidade não foi avaliada.
- **Egoísmo não detectado:** a reputação isolada não penaliza nós que não colaboram com avaliações.

---

## Trabalhos futuros

- Realizar mais de 5 execuções por cenário, para obter dados estatisticamente mais confiáveis.
- Explorar a distribuição do Fogbed em várias máquinas (workers), com cenários de alta taxa de requisições e avaliações, para avaliar escalabilidade.
- Testar outros algoritmos de confiança e outros valores dos parâmetros (pesos e limiares), analisando o impacto na identificação de nós maliciosos.
- Incorporar um mecanismo para detectar a não colaboração (nó egoísta).
- Adicionar exploração à escolha da dupla gateway/dispositivo (por exemplo, sorteio entre empates ou uma fração de escolhas aleatórias).
- Atualizar `fogbed_exec.py` para a imagem com a API embutida e o MQTT direto, e executar em vários workers.

---

## Estrutura de diretórios

```text
myproject/
├── Dockerfile                  # Build da API Tangle (Go) + imagem do gateway Zato
├── README.md
├── docs/
│   └── images/                 # Figuras usadas neste README
│       ├── arquitetura.png
│       ├── sequencia-dispositivo-gateway.png
│       ├── reputacao-cenario1-malicioso-30.png
│       ├── reputacao-cenario2-malicioso-70.png
│       ├── reputacao-cenario3-perturbador.png
│       └── reputacao-cenario4-egoista.png
├── config/
│   ├── enmasse/enmasse.yaml    # REST + external_scheduler
│   ├── auto-generated/env.ini  # Gerado pelo script do Fogbed (deploy)
│   └── python-reqs/requirements.txt
└── impl/
    ├── scripts/
    │   ├── fogbed_tangle_local.py                # Tangle local (Hornet + coordenador + spammer)
    │   ├── exec_architecture_local_continuous.py # Rodadas com gateways e dispositivos + coleta
    │   ├── data_relatorio_final/run_<N>/         # Saída de cada rodada
    │   └── fogbed_exec.py                        # Execução distribuída (arquitetura anterior)
    └── src/
        ├── services/           # Serviços Zato (lógica principal)
        └── archives/           # devices.json → mapeamento no container
```

---

## Referência rápida de desenvolvimento

- **Serviços:** implementados em Python, herdando `zato.server.service.Service`; o nome é registrado no atributo `name` e mapeado em `enmasse.yaml`.
- **Invocações internas:** `self.invoke('soft-iot....')` e `self.invoke_async(...)` entre serviços do mesmo cluster Zato. Fora de um serviço (por exemplo, em threads do MQTT ou do armazenamento), use `server.invoke_async(nome, payload, None)`, lembrando que ele roda na própria thread que chama.
- **Imports entre módulos:** importe outros módulos `soft_iot_*` dentro da função que os usa, não no topo do arquivo (ver [Observações e limitações](#observações-e-limitações)).
- **Estado de experimento:** requisições, respostas e reputação local no SQLite do gateway; dados de sensores em `soft_iot_data.db`; histórico global de reputação na Tangle.
- **Build:** reconstruir a imagem a partir do `Dockerfile` e atualizar a tag usada no Fogbed quando houver alterações nos serviços.

Detalhes de configuração do cluster Zato/Enmasse seguem a documentação da imagem base `esb-zato-soft-iot`.

---

## Referências

- IOTA Foundation. *IOTA Documentation*. <https://docs.iota.org/>
- IOTA Foundation. *iota.go*. <https://github.com/iotaledger/iota.go>
- LARSID. *Fogbed*. <https://larsid.github.io/fogbed/>
- A. C. S. Santos. *Sistema de Reputação Distribuído com Blockchain para Ecossistemas IoT*. Trabalho de Conclusão de Curso, Universidade Estadual de Feira de Santana.
- A. K. Jain. "Data clustering: 50 years beyond K-means". *Pattern Recognition Letters*, vol. 31, no. 8, pp. 651–666, 2010.
- Zato Source. *zato*. <https://github.com/zatosource/zato>
