"""Estado do cluster Spark standalone (corre dentro do contentor jupyter).

    python3 cluster_status.py                 mostra workers, cores e apps a correr
    python3 cluster_status.py --require-free  sai com erro se houver apps a correr
    python3 cluster_status.py --wait-free 30  espera até 30 s que o cluster fique livre
"""
import argparse
import json
import sys
import time
import urllib.request

URL = "http://spark-master:8080/json/"


def state():
    with urllib.request.urlopen(URL, timeout=5) as r:
        return json.load(r)


def show(d):
    apps = d.get("activeapps", [])
    print(f"workers vivos: {d.get('aliveworkers')} | "
          f"cores em uso: {d.get('coresused')}/{d.get('cores')} | "
          f"apps a correr: {len(apps)}")
    for a in apps:
        print(f"  - {a.get('name')} ({a.get('id')}): {a.get('cores')} cores")


p = argparse.ArgumentParser()
p.add_argument("--require-free", action="store_true",
               help="sai com erro se houver aplicações a correr")
p.add_argument("--wait-free", type=int, default=0, metavar="SEG",
               help="espera até SEG segundos que o cluster fique livre")
a = p.parse_args()

try:
    d = state()
except Exception as e:
    sys.exit(f"Não consegui contactar o master em {URL}: {e}")

deadline = time.time() + a.wait_free
while d.get("activeapps") and time.time() < deadline:
    time.sleep(5)
    d = state()

show(d)
if (a.require_free or a.wait_free) and d.get("activeapps"):
    sys.exit("\nCluster ocupado: há uma SparkSession aberta a segurar os cores.\n"
             "Fecha os kernels do Jupyter (Kernel > Shut Down All Kernels) "
             "ou corre 'make free-cluster'.")
