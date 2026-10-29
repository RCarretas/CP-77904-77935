# Projeto 1 — Large-Scale Analytics with Spark (NYC Yellow Taxi)

Pipeline em PySpark (API de DataFrames) com as operações M1, M2, M3, O1 e O2.
**Esta é a versão base (baseline), sem otimizações.** Cada otimização posterior deve ser medida contra ela.

## Dados

Não estão no repositório. Descarregar para `data/raw/`:

- Viagens (um ficheiro por mês): `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
- Tabela de zonas: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

Fonte: NYC TLC Trip Record Data (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Data de download: _(preencher)_.

## Ambiente

- Spark / PySpark 4.2.0, Java 17, Python 3.14 (local); imagem `spark:python3` no docker
- `pip install -r requirements.txt`

## Estrutura

```
apps/
  pipeline.py         ponto de entrada (argumentos, sessão Spark, tempos)
  ingest.py           leitura dos Parquet com esquema imposto + tabela de zonas
  m1_clean.py         M1: limpeza e estatísticas de limpeza por ano
  m2_temporal.py      M2: procura por borough / dia da semana / hora
  m3_zones.py         M3: estatísticas por zona e mês, janelas
  o1_fare_anomaly.py  O1: anomalias de tarifa por km
  o2_od_flow.py       O2: fluxos origem-destino
data/raw/             dados (fora do git)
output/<run_id>/      resultados de cada execução (fora do git)
runs.csv              tempo de cada operação em cada execução (fora do git)
```

## Como correr

```bash
# teste rápido: 1% de janeiro de 2020
python apps/pipeline.py --input data/raw --zones data/raw/taxi_zone_lookup.csv \
    --years 2020 --months 1 --sample 0.01 --master "local[*]"

# um ano completo, com event log para rever as métricas no Spark History Server
spark-submit --master "local[*]" --driver-memory 4g apps/pipeline.py \
    --input data/raw --zones data/raw/taxi_zone_lookup.csv --years 2020 --event-log spark-events
```

Argumentos principais:

| argumento | significado |
|---|---|
| `--input` | pasta(s) e/ou ficheiros `yellow_tripdata_YYYY-MM.parquet` |
| `--zones` | caminho do `taxi_zone_lookup.csv` |
| `--years`, `--months` | filtram os ficheiros a ler |
| `--sample` | fração dos registos (ex.: 0.01) |
| `--ops` | operações a correr (por defeito `m1 m2 m3 o1 o2`) |
| `--conf CHAVE=VALOR ...` | configurações Spark extra |
| `--event-log PASTA` | ativa o event log do Spark |
| `--wait` | no fim, espera por Enter para se poder ver o Spark UI em `localhost:4040` |

Cada execução acrescenta ao `runs.csv` uma linha por operação, com o tempo em segundos, o commit do git e os parâmetros.

## Decisões de implementação

**M1**
- Esquema: cada ficheiro mensal é lido e convertido separadamente para os tipos do enunciado (os tipos mudam entre anos), e os meses são juntos com `unionByName`.
- Os critérios são contados de forma **independente**: um registo que falha dois critérios conta nos dois. `removed_any_criterion` conta cada registo removido uma só vez.
- Critérios extra: `null_values` (nulo em qualquer coluna usada) e `pickup_outside_file_month` (pickup fora do mês do ficheiro; os ficheiros do TLC trazem algumas viagens com datas erradas).
- O ano das estatísticas é o ano do ficheiro.

**M2**: `day_of_week` segue o Spark (`dayofweek`: 1 = domingo ... 7 = sábado). A duração é em minutos.

**M3**
- Quota móvel de 3 meses = viagens da zona nos meses m-2..m ÷ viagens do borough nos mesmos meses.
- Média e desvio-padrão móveis da zona calculados sobre os **3 meses anteriores** (sem o atual); a flag é `|viagens − média| > 1.5 σ`. Sem histórico suficiente, a flag é falsa.
- Meses sem viagens numa zona não têm linha (contam como ausentes, não como zero).

**O1**: tarifa por km = `fare_amount / (trip_distance × 1.609344)`. Anomalia: acima de média + 3σ do seu bucket (zona de pickup, hora). Fim de semana = sábado e domingo.

**O2**: top-20 rotas por borough de pickup, pelo total de viagens no período. Inversão Q1/Q3: tendência Q1 = quota(março) − quota(janeiro), tendência Q3 = quota(setembro) − quota(julho), e a rota é sinalizada quando os sinais são opostos (só funciona com esses 4 meses presentes).

## Baseline

Sem `cache`, sem `broadcast` explícito, sem `repartition` e com as configurações por defeito do Spark (incluindo AQE e o broadcast automático de tabelas pequenas). Os dados limpos não são guardados em cache, por isso cada operação volta a ler e limpar os Parquet.

## Agradecimentos

O código inicial foi gerado com ajuda de um assistente de IA (Claude) e revisto pelo autor.
