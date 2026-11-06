# =====================================================================
#  Infraestrutura do cluster Spark (Docker)
#  Correr a partir de docker-spark-env/  ou da raiz com:  make -C docker-spark-env <alvo>
# =====================================================================

WORKERS ?= 3                      # nº de workers (cada um com 2 cores / 2 GB)
DC      := docker compose
JUPYTER := jupyter
USER_ID := $(shell id -u)
GROUP_ID:= $(shell id -g)

.DEFAULT_GOAL := help
.PHONY: help build up down restart status logs shell urls fix-perms free-cluster clean

help:
	@echo "Infraestrutura (docker-spark-env):"
	@echo "  make up [WORKERS=3]   constroi a imagem e arranca master + workers + jupyter"
	@echo "  make down             para e remove os contentores"
	@echo "  make restart          down + up"
	@echo "  make status           contentores + estado do cluster (workers, cores, apps a correr)"
	@echo "  make logs             logs do master e dos workers (Ctrl+C para sair)"
	@echo "  make shell            abre uma bash dentro do contentor jupyter"
	@echo "  make urls             mostra os enderecos das UIs"
	@echo "  make free-cluster     reinicia o jupyter (mata kernels/SparkSessions que seguram os cores)"
	@echo "  make fix-perms        devolve ao teu utilizador os ficheiros criados pelo root nos contentores"
	@echo "  make clean            down + apaga volumes e imagem do jupyter"

build:
	$(DC) build

up:
	$(DC) up -d --build --scale spark-worker=$(WORKERS)
	@$(MAKE) --no-print-directory urls

down:
	$(DC) down

restart: down up

status:
	@$(DC) ps
	@echo
	@docker exec $(JUPYTER) python3 /workspace/apps/cluster_status.py

logs:
	$(DC) logs -f spark-master spark-worker

shell:
	docker exec -it $(JUPYTER) bash

urls:
	@echo "Spark Master UI : http://localhost:8080"
	@echo "Jupyter         : http://localhost:8888  (token: token_msc)"
	@echo "Spark App UI    : http://localhost:4040  (enquanto uma aplicacao corre)"

free-cluster:
	@echo "A reiniciar o contentor jupyter (fecha todos os kernels e SparkSessions abertas)..."
	docker restart $(JUPYTER)
	@sleep 3
	@docker exec $(JUPYTER) python3 /workspace/apps/cluster_status.py --wait-free 30

fix-perms:
	docker exec $(JUPYTER) chown -R $(USER_ID):$(GROUP_ID) /workspace/apps /data/output

clean:
	$(DC) down -v --rmi local
