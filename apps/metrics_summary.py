"""Resumo do metrics.csv: nº de repetições, mediana, mínimo e máximo do wall_time_s
por run e operação (inclui a linha TOTAL de cada run).

    python3 metrics_summary.py /data/output/metrics.csv [run_name ...]
"""
import sys

import pandas as pd

path = sys.argv[1] if len(sys.argv) > 1 else "/data/output/metrics.csv"
runs = sys.argv[2:]

df = pd.read_csv(path)
if runs:
    df = df[df["run_name"].isin(runs)]
if df.empty:
    sys.exit("Sem linhas para mostrar.")

g = (df.groupby(["run_name", "step"], sort=False)["wall_time_s"]
       .agg(n="count", mediana="median", min="min", max="max")
       .round(1))

pd.set_option("display.width", 160)
print(g.to_string())

# tabela compacta: mediana por operação, uma coluna por run
print("\nMediana do wall_time_s (s):")
print(g["mediana"].unstack("run_name").to_string())
