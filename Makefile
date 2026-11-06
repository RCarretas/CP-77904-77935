# =====================================================================
#  Correr versoes do pipeline com parametros diferentes
#  (o cluster tem de estar ligado:  make up)
#
#  Exemplos:
#    make final                               versao final, dataset completo
#    make run NB=baseline RUN=baseline REPS=3 baseline, 3 repeticoes
#    make run MONTHS=1 RUN=final-1mes         versao final so com janeiro
#    make run NB=02-persist FRACTION=0.1      outro notebook, 10% dos dados
#    make bench                               baseline x3 + final x3
#    make scale                               versao final com 10/25/50/100% dos dados
#    make summary                             mediana dos tempos por run/operacao
# =====================================================================

# ---- parametros (todos opcionais; vazio = valor por defeito do notebook) ----
NB       ?= versaofinal     # nome do notebook em apps/, sem .ipynb
RUN      ?= $(NB)           # etiqueta da run no metrics.csv
REPS     ?= 1               # numero de repeticoes
YEARS    ?=                 # ex.: "2019"  ou  "2018 2019 2020"
MONTHS   ?=                 # ex.: "1"     ou  "1 2 3"
FRACTION ?=                 # ex.: 0.1
OPS      ?=                 # ex.: "M2 M3"
EXTRA    ?=                 # outros argumentos passados tal e qual ao script

# ---- configuracao do cluster ----
JUPYTER  := jupyter
APPS     := /workspace/apps
SUBMIT   := /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
            --conf spark.executor.memory=1536m --conf spark.driver.memory=2g

# remove espacos/comentarios inline dos valores
strip    = $(strip $(1))
ARGS     := --run-name $(call strip,$(RUN)) \
            $(if $(call strip,$(YEARS)),--years $(YEARS)) \
            $(if $(call strip,$(MONTHS)),--months $(MONTHS)) \
            $(if $(call strip,$(FRACTION)),--fraction $(FRACTION)) \
            $(if $(call strip,$(OPS)),--ops $(OPS)) \
            $(EXTRA)
NBNAME   := $(call strip,$(NB))

.DEFAULT_GOAL := help
.PHONY: help convert check-free run baseline final bench exp-1mes scale summary up down status free-cluster fix-perms

help:
	@echo "Correr o pipeline (dentro do contentor jupyter, com spark-submit):"
	@echo "  make run [NB=..] [RUN=..] [REPS=..] [YEARS=..] [MONTHS=..] [FRACTION=..] [OPS=..] [EXTRA=..]"
	@echo "  make baseline         baseline.ipynb      (REPS=$(call strip,$(REPS)))"
	@echo "  make final            versaofinal.ipynb   (REPS=$(call strip,$(REPS)))"
	@echo "  make bench            baseline x3 + final x3"
	@echo "  make exp-1mes         versao final so com janeiro (3 ficheiros), REPS=3"
	@echo "  make scale            final com FRACTION = 0.1 0.25 0.5 1.0"
	@echo "  make summary          mediana/min/max do wall_time_s por run e operacao"
	@echo
	@echo "Infraestrutura:  make up | down | status | free-cluster | fix-perms   (ver docker-spark-env/Makefile)"

# ---- passos base ----
convert:
	@docker exec $(JUPYTER) bash -c "cd $(APPS) && jupyter nbconvert --to python $(NBNAME).ipynb --log-level=WARN"

check-free:
	@docker exec $(JUPYTER) python3 $(APPS)/cluster_status.py --require-free

run: convert check-free
	@for i in $$(seq 1 $(call strip,$(REPS))); do \
	  echo ""; echo ">>> $(NBNAME) | repeticao $$i/$(call strip,$(REPS)) | $(strip $(ARGS))"; \
	  docker exec $(JUPYTER) bash -c "cd $(APPS) && $(SUBMIT) $(NBNAME).py $(strip $(ARGS))" || exit 1; \
	done
	@$(MAKE) --no-print-directory fix-perms

# ---- atalhos ----
baseline:
	@$(MAKE) --no-print-directory run NB=baseline RUN=baseline

final:
	@$(MAKE) --no-print-directory run NB=versaofinal RUN=final

bench:
	@$(MAKE) --no-print-directory run NB=baseline    RUN=baseline REPS=3
	@$(MAKE) --no-print-directory run NB=versaofinal RUN=final    REPS=3
	@$(MAKE) --no-print-directory summary

exp-1mes:
	@$(MAKE) --no-print-directory run NB=versaofinal RUN=final-1mes MONTHS=1 REPS=3

scale:
	@for f in 0.1 0.25 0.5 1.0; do \
	  $(MAKE) --no-print-directory run NB=$(NBNAME) RUN=$(NBNAME)-f$$f FRACTION=$$f REPS=$(call strip,$(REPS)) || exit 1; \
	done
	@$(MAKE) --no-print-directory summary

summary:
	@docker exec $(JUPYTER) python3 $(APPS)/metrics_summary.py /data/output/metrics.csv

# ---- infraestrutura (delegado em docker-spark-env/Makefile) ----
up down status free-cluster fix-perms:
	@$(MAKE) --no-print-directory -C docker-spark-env $@