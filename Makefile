########################################
##        GPS-mobility Pipeline       ##
##     Ollin Demian Langle Chimal     ##
########################################

.PHONY: clean data lint init deps sync_to_gs sync_from_gs

########################################
##            Variables               ##
########################################

## Project Directory
PROJ_DIR:=$(shell pwd)

PROJECT_NAME:=$(shell cat .project-name)
PROJECT_VERSION:=$(shell cat .project-version)
DATE:=$(shell date +'%Y-%m-%d')

## Python Version
VERSION_PYTHON:=$(shell python -V)

SHELL := /bin/bash

## Airflow variables
AIRFLOW_GPL_UNIDECODE := yes

########################################
##       Environment Tasks            ##
########################################

init: prepare ##@dependencias Prepara la computadora para el funcionamiento del proyecto

prepare: deps
	yes | conda create --name ${PROJECT_NAME}_venv python=3.7
	conda activate ${PROJECT_NAME}_venv
#	pyenv virtualenv ${PROJECT_NAME}_venv
#	pyenv local ${PROJECT_NAME}_venv

#pyenv: .python-version
#	@pyenv install $(VERSION_PYTHON)

deps: pip airdb

pip: requirements.txt
	@conda install -c conda-forge --file $<
	# @pip install -r $<

airdb:
	@source .env
	--directory=$(AIRFLOW_HOME)
	@airflow db init

info:
	@echo Project: $(PROJECT_NAME) ver. $(PROJECT_VERSION) in $(PROJ_DIR)
	@python --version
	@pip --version

deldata:
	@ yes | rm data/raw/* data/clean/*

runpipeline:
	@airflow dags backfill gpspipeline -s $(DATE)

prune:
	@docker container prune
########################################
##          Infrastructure            ##
##    	   Execution Tasks            ##
########################################

create: ##@infrastructure Builds the required containers
	$(MAKE) -c=infrastructure --directory=infrastructure build

start: ##@infraestructura Starts the Docker Compose and build the images if required
	$(MAKE) --directory=infrastructure init

stop: ##@infrastructure Stops the Docker Compose infrastructure
	$(MAKE) --directory=infrastructure stop

status: ##@infrastructure Infrastructure status
	$(MAKE) --directory=infrastructure status

destroy: ##@infrastructure Delete the docker images
	$(MAKE) --directory=infrastructure clean
	@docker rmi ollin18/gpspipeline:0.1 gpspipeline:latest

nuke: ##@infrastructure Destroy all infrastructure (TODO)
	$(MAKE) --directory=infrastructure nuke

neo4j:
	@$(MAKE) --directory=infrastructure init

# neo4jrebuild:
#     @$(MAKE) --directory=infrastructure rebuild

ingest:
	@$(MAKE) --directory=infrastructure ingester

dockerbuild:
	@$(MAKE) --directory=infrastructure build

########################################
##           Data Sync Tasks          ##
########################################

sync_to_gs: ##@data Sincroniza los datos hacia GCP GS
	@gsutil -m rsync -R data/ $(GS_BUCKET)/data/

sync_from_gs: ##@data Sincroniza los datos desde GCP GS
	@gsutil -m rsync -R $(GS_BUCKET)/data/ data/

sync_to_s3: ##@data Sincroniza los datos hacia AWS S3
	@aws s3 sync data/ s3://$(S3_BUCKET)/data/

sync_from_s3: ##@data Sincroniza los datos desde AWS S3
	@aws s3 sync s3://$(S3_BUCKET)/data/ data/

########################################
##          Project Tasks             ##
########################################

run:       ##@proyecto Ejecuta el pipeline de datos
	$(MAKE) --directory=$(PROJECT_NAME) run

setup: build install ##@proyecto Crea las imágenes del pipeline e instala el pipeline como paquete en el PYTHONPATH

build:
	$(MAKE) --directory=$(PROJECT_NAME) build

install:
	@pip install --editable .

uninstall:
	@while pip uninstall -y ${PROJECT_NAME}; do true; done
	@python setup.py clean

## Verificando dependencias
## Basado en código de Fernando Cisneros @ datank

EXECUTABLES = docker docker-compose docker-machine rg pip
TEST_EXEC := $(foreach exec,$(EXECUTABLES),\
				$(if $(shell which $(exec)), some string, $(error "${BOLD}${RED}ERROR${RESET}: $(exec) is not in the PATH")))
