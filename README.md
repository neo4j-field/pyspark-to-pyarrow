# simple demo of PySpark to PyArrow-on-GDS

## required env args
* `NEO4J_HOST`: _hostname or ip_ of target Neo4j system
* `NEO4J_PASS`: password of `neo4j` user on the system
* `STAGING`: GCS storage bucket to use as the staging point for the Dataproc job

## prereqs
* `gcloud` tooling auth'd & ready
* you `gcloud` config has `dataproc.region` set to your default region (if not, add a `REGION` env arg)

## make rocket go now

```
$ NEO4J_HOST="voutila-gds-arrow-demo" \
  NEO4J_PASS=mysupersecretpassword \
  STAGING=gs://neo4j_voutila/staging \
  ./run.sh job.py
```
