# Projeto 1 — Large-Scale Analytics with Spark

Pipeline PySpark sobre os NYC TLC Yellow Taxi Trip Records.
Operações: **M1, M2, M3** (obrigatórias) e **O1, O2** (opcionais).

## Dados

Não estão no repositório. Descarregar para `data/`:

- Viagens: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
- Zonas: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

Exemplo:

```bash
cd data
for y in 2018 2019 2020; do for m in $(seq -w 1 12); do
  wget -nc https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${y}-${m}.parquet
done; done
wget -nc https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Data do download: _(preencher)_.

## Ambiente

- Docker compose em `docker-spark-env/` (imagem `spark:python3`, 1 master + 1 worker com 2 cores / 2 GB, Jupyter)
- Versão do Spark: _(preencher com o output de `spark.version`)_

```bash
cd docker-spark-env
docker compose up -d --build
```

## Correr

Pelo notebook: `apps/misturas.ipynb` (Jupyter em http://localhost:8888, token `token_msc`).

Ou com spark-submit dentro do contentor:

```bash
docker exec -it jupyter /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=1536m --conf spark.driver.memory=2g \
  /workspace/apps/pipeline.py \
  --input /data --output /data/output \
  --years 2018 2019 2020 --fraction 1.0 --run-name baseline
```

Parâmetros principais:

| Parâmetro | Significado |
|---|---|
| `--input` | pasta com os Parquet e o CSV de zonas |
| `--output` | pasta de saída |
| `--years`, `--months` | anos/meses a ler (ficheiros em falta são ignorados) |
| `--fraction` | amostra dos registos (ex.: `0.1`) |
| `--ops` | subconjunto de `M1 M2 M3 O1 O2` |
| `--run-name` | etiqueta da run no `metrics.csv` |
| `--event-log-dir` | ativa o event log (History Server) |

Cada run acrescenta uma linha por operação a `<output>/metrics.csv`. Cada linha tem o tempo, os stages, as tasks, o shuffle read/write e o spill.

## Configuração não-default

Baseline: nenhuma, além de `spark.executor.memory=1536m` e `spark.driver.memory=2g`.
