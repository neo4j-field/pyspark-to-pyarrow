#!/bin/sh

# Currently using a preview that targets Spark 2.4
SPARK_BQ_URI="gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.2.jar"

# GCP config
STAGING="${STAGING:=}"
REGION="${REGION:=$(gcloud config get dataproc/region)}"
CLUSTER="${CLUSTER:=voutila-dp-test}"
PROJECT="${PROJECT:=$(gcloud config get project)}"
CONDA_ENV_YAML="${CONDA_ENV_YAML:=environment.yml}"

# Neo4j config
NEO4J_URL="${NEO4J_HOST:=}"
NEO4J_USER="${NEO4J_USER:=neo4j}"
NEO4J_PASS="${NEO4J_PASS:=password}"
DATABASE="${DATABASE:=neo4j}"

usage () {
    echo "usage: run.sh [pyspark-job-file]" >&2
    echo "" >&2
    echo "required environment config (cause I'm lazy!):" >&2
    echo "      STAGING: GCS location for staging job artifacts" >&2
    echo "    NEO4J_HOST: Neo4j bolt host (note: NOT url!)" >&2
    echo "optional environment config:" >&2
    echo "           CLUSTER: Dataproc cluster to run the job (default: ${CLUSTER})" >&2
    echo "    CONDA_ENV_YAML: file for bootstrapping Dataproc's conda env" >&2
    echo "            REGION: GCP region for the cluster (default: ${REGION})" >&2
    exit 1
}

fail () {
    msg="${1:=unknown error}"
    echo "Uh oh! ${msg}" >&2
    if [ "${showusage}" ]; then
        usage;
    else
        exit 1
    fi # dead
}

create_cluster () {
    if [ ! -f "${CONDA_ENV_YAML}" ]; then
        fail "CONDA_ENV_YAML must be set if you want me to create a cluster!";
    fi

    if ! $(gsutil cp "${CONDA_ENV_YAML}" "${STAGING}/"); then
        fail "Failed to copy ${CONDA_ENV_YAML} to ${STAGING}!}"
    fi

    if ! $(gcloud dataproc clusters create --region="${REGION}" \
                  --image-version=2.0 \
                  --num-masters=1 \
                  --num-workers="2" \
                  --max-idle="1d" \
                  --properties="dataproc:conda.env.config.uri=${STAGING}/${CONDA_ENV_YAML}" \
                  "${CLUSTER}"); then
        fail "Failed to create Dataproc cluster ${CLUSTER} in ${REGION}."
    fi
}

# Our only positional argument
if [ "${#}" -ne 1 ]; then usage; fi
JOBFILE="${1:=}"

# Inspect required toggles
if [ ! -f "${JOBFILE}" ]; then usage; fi
if [ -z "${NEO4J_HOST}" ]; then fail "no NEO4J_HOST set!"; fi
if [ -z "${STAGING}" ]; then fail "no STAGING set, please specify a GCS uri!"; fi

# Check for or create Dataproc Cluster
if ! $(gcloud dataproc clusters list --region="${REGION}" --filter="clusterName=${CLUSTER}" | grep "${CLUSTER}" > /dev/null 2>&1); then
    echo "No cluster ${CLUSTER} found in ${REGION}. Creating it..."
    create_cluster
fi

# Deploy our job artifacts using rsync
# gsutil rsync -x "venv" "$(pwd)" "${STAGING}/"
gsutil cp "${JOBFILE}" "${STAGING}/"

PY_FILES="$(ls *.py | sed 's/.*/&,/' | tr -d '\n' | sed 's/,,//')"
if [ ! -z "${PY_FILES}" ]; then echo "...including Python files ${PY_FILES}"; fi

# Submit DataProc job!
gcloud dataproc jobs submit pyspark \
       "${STAGING}/${JOBFILE}" \
       --region="${REGION}" \
       --cluster="${CLUSTER}" \
       --jars="${SPARK_BQ_URI}" \
       --py-files="${PY_FILES}" \
       -- "${NEO4J_HOST}" "${NEO4J_USER}" "${NEO4J_PASS}"
