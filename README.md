# Projeto 1 — Large-Scale Analytics with Spark

Pipeline PySpark sobre os NYC TLC Yellow Taxi Trip Records (2018–2020, meses 1–9).
Operações: **M1, M2, M3** (obrigatórias) e **O1, O2** (opcionais), numa versão
**baseline** e numa versão **otimizada** (`versaofinal`).

---

## 1. Requisitos

- Docker com Docker Compose (`docker compose version`)
- `make` (`make --version`; se faltar: `sudo apt install make`)

Tudo o resto (Spark, Python, Jupyter) corre dentro dos contentores.

## 2. Dados

Não estão no repositório. Descarregar para `data/`:

```bash
cd data
for y in 2018 2019 2020; do for m in $(seq -w 1 9); do
  wget -nc https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${y}-${m}.parquet
done; done
wget -nc https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Data do download: _(preencher)_.

## 3. Estrutura

```
Projeto1/
├── Makefile                  correr versões do pipeline (make run, bench, scale, summary)
├── docker-spark-env/
│   ├── Makefile              infraestrutura (make up, down, status, free-cluster…)
│   ├── docker-compose.yml    spark-master + spark-worker (×N) + jupyter
│   └── Dockerfile            imagem do jupyter (spark:python3 + jupyterlab + pandas)
├── apps/                     montado em /workspace/apps nos contentores
│   ├── baseline.ipynb        versão baseline
│   ├── versaofinal.ipynb     versão otimizada
│   ├── *.ipynb               versões intermédias (uma otimização de cada vez)
│   ├── spark_profiler.py     recolhe as métricas de cada operação
│   ├── cluster_status.py     estado do cluster (workers, cores, apps a correr)
│   └── metrics_summary.py    resumo do metrics.csv (mediana/min/max)
└── data/                     montado em /data (input e /data/output)
```

## 4. Infraestrutura

Todos os comandos correm a partir da raiz `Projeto1/`.

| Comando | O que faz |
|---|---|
| `make up` | constrói a imagem e arranca master + 3 workers + jupyter |
| `make up WORKERS=2` | o mesmo com outro número de workers (cada um: 2 cores, 2 GB) |
| `make status` | contentores e estado do cluster: workers, cores em uso, apps a correr |
| `make free-cluster` | reinicia o jupyter, fechando kernels/SparkSessions que seguram os cores |
| `make fix-perms` | devolve ao teu utilizador os ficheiros criados pelo `root` nos contentores |
| `make down` | para e remove os contentores |
| `make -C docker-spark-env logs` | logs do master e dos workers |
| `make -C docker-spark-env shell` | bash dentro do contentor jupyter |
| `make -C docker-spark-env clean` | `down` + apaga volumes e a imagem do jupyter |

Endereços:

- Spark Master UI: http://localhost:8080
- Jupyter: http://localhost:8888 (token `token_msc`)
- Spark App UI: http://localhost:4040 (só enquanto uma aplicação corre)

Ambiente usado nas medições: Spark 4.2.0, 3 workers × 2 cores × 2 GB,
`spark.executor.memory=1536m`, `spark.driver.memory=2g`.

## 5. Correr o pipeline

### Com `make` (recomendado)

`make run` converte o notebook para `.py`, verifica que o cluster está livre e corre-o com
`spark-submit` dentro do contentor jupyter. No fim corrige as permissões dos ficheiros.

```bash
make run [NB=..] [RUN=..] [REPS=..] [YEARS=..] [MONTHS=..] [FRACTION=..] [OPS=..] [EXTRA=..]
```

| Parâmetro | Por defeito | Significado |
|---|---|---|
| `NB` | `versaofinal` | notebook em `apps/`, sem `.ipynb` |
| `RUN` | igual a `NB` | etiqueta da run no `metrics.csv` |
| `REPS` | `1` | número de repetições |
| `YEARS` | 2018 2019 2020 | anos a ler, ex.: `YEARS="2019 2020"` |
| `MONTHS` | 1–9 | meses a ler, ex.: `MONTHS=1` (ficheiros em falta são ignorados) |
| `FRACTION` | `1.0` | fração dos registos, ex.: `FRACTION=0.1` |
| `OPS` | todas | subconjunto de `M1 M2 M3 O1 O2`, ex.: `OPS="M2 M3"` |
| `EXTRA` | — | outros argumentos passados tal e qual ao script |

Valores com espaços levam aspas: `YEARS="2019 2020"`. Os nomes dos parâmetros
distinguem maiúsculas (`WORKERS`, não `workers`).

Atalhos:

| Comando | O que corre |
|---|---|
| `make baseline` | `baseline.ipynb` (`RUN=baseline`) |
| `make final` | `versaofinal.ipynb` (`RUN=final`) |
| `make bench` | baseline ×3 + final ×3, seguido de `make summary` |
| `make exp-1mes` | versão final só com janeiro de cada ano (3 ficheiros), ×3 |
| `make scale` | versão final com `FRACTION` = 0.1, 0.25, 0.5 e 1.0, seguido de `make summary` |
| `make summary` | mediana, mínimo e máximo do `wall_time_s` por run e operação |

Exemplos:

```bash
make final REPS=3
make run NB=baseline RUN=baseline REPS=3
make run MONTHS=1 RUN=final-1mes
make run NB=02-persist FRACTION=0.1 RUN=persist-10pct
make run OPS="M2 M3" RUN=final-m2m3
```

### Pelo Jupyter

Abrir http://localhost:8888, abrir o notebook e fazer *Run All*. No notebook o
`argparse` usa os valores por defeito; para outros valores, mudar a célula de parâmetros:

```python
args = p.parse_args(["--months", "1", "--run-name", "teste"] if IN_NOTEBOOK else None)
```

**Importante:** uma SparkSession aberta no Jupyter fica com todos os cores do cluster até
o kernel ser fechado. Antes de usar `make run`, fechar os kernels
(*Kernel → Shut Down All Kernels*) ou correr `make free-cluster`.

## 6. Métricas

Cada run acrescenta uma linha por operação, mais uma linha `TOTAL`, a
`data/output/metrics.csv`. Cada linha tem a etiqueta (`run_name`), os parâmetros da run, o
tempo (`wall_time_s`), stages, tasks, shuffle read/write, spill e GC.

```bash
make summary
```

Para comparações, correr pelo menos 3 repetições por configuração e usar a mediana: o
cluster corre em contentores num único portátil e o tempo varia ±10% entre runs idênticas.

## 7. Configuração não-default

- **Baseline:** apenas `spark.executor.memory=1536m` e `spark.driver.memory=2g`.
- **Versão final:** além do anterior, `spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS`
  na escrita do `trips_clean`. As restantes otimizações estão no código: materialização do
  dataset limpo, `persist` com `coalesce`, `broadcast` explícito e agregação antes do join.

## 8. Problemas comuns

| Sintoma | Causa | Solução |
|---|---|---|
| `Initial job has not accepted any resources` | outra SparkSession (normalmente um kernel do Jupyter) segura os cores | `make free-cluster` |
| `make run` pára com "Cluster ocupado" | o mesmo, detetado antes de arrancar | `make free-cluster` |
| `git add`: `Permissão recusada` | ficheiros criados pelo `root` nos contentores | `make fix-perms` |
| `UnknownHostException: spark-master` | `spark-submit` corrido no portátil, fora do Docker | usar `make run`, que corre dentro do contentor |
| `make: Sem regra para fazer o alvo` | o ficheiro não se chama exatamente `Makefile`, ou falta `docker-spark-env/Makefile` | renomear para `Makefile` (maiúsculas contam) |
| `Makefile: missing separator` | as linhas dos comandos têm espaços em vez de TAB | repor os TABs (ou voltar a descarregar o ficheiro) |
