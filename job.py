import os
import sys

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf

import pyarrow as pa
import pyarrow.flight as flight
from graphdatascience import GraphDataScience

import gdsarrow # local


def load_rows_as_tables(client: flight.FlightClient, desc: flight.FlightDescriptor):
    """
    Higher-order function that converts PySpark Rows into PyArrow Tables and
    feeds them to Neo4j.
    """
    # todo: move out
    client = flight.FlightClient(
        flight.Location.for_grpc_tcp(arrow_host, arrow_port)
    )

    def loader(iterator):
        # xxx this is a hack and will not scale
        data = dict()
        for row in iterator:
            for field in row.asDict():
                data.get(field, []).append(row[field])
        keys = data.keys()
        arrays = [pa.array(data[k]) for k in keys]
        table = pa.table(arrays, names=keys)
        gdsarrow.write_table(client, desc, table)
        yield len(batch), desc

    return loader


### Set up Neo4j stuff
if len(sys.argv) < 4:
    print("uhhh see usage?")
    os.exit(1)

_, NEO4J_HOST, NEO4J_USER, NEO4J_PASS = sys.argv[:4]
gds = GraphDataScience(NEO4J_HOST, auth=(NEO4J_USER, NEO4J_PASS))
print(gds.run_cypher("RETURN 1;"))


### Set up Spark stuff
spark = (
    SparkSession.builder
    .appName("PySpark to PyArrow GDS Example")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.execution.arrow.pyspark.maxRecordsPerBatch", 10_000)
    .getOrCreate()
)
nodes = (
    spark.read
    .format("bigquery")
    .load("neo4j-se-team-201905.fraud_demo_data.user")
    .repartition(8)
    .cache()
)
edges =  (
    spark.read
    .format("bigquery")
    .load("neo4j-se-team-201905.fraud_demo_data.p2p")
    .repartition(8)
    .cache()
)
print(f">>> Starting with {nodes.count():,} nodes, {edges.count():,} edges")


### Start flight server
gds.run_cypher("CALL gds.alpha.flightServer.start();")

client = flight.FlightClient(flight.Location.for_grpc_tcp(arrow_host, arrow_port))
gdsarrow.send_action(client, "CREATE_GRAPH",
                     {"name": "test"}, NEO4J_HOST, 4242)

### Load Nodes
node_descriptor = { "name": "test", "entity_type": "node" }
nodes = nodes.rdd.mapPartitions(load_rows_as_tables(client, node_descriptor))
print(f"loaded nodes: {nodes.collect()}")

gdsarrow.send_action(client, "NODE_LOAD_DONE",
                     {"name": "test"}, NEO4J_HOST, 4242)

### Load Edges
edge_descriptor = { "name": "test", "entity_type": "relationship" }
edges = edges.rdd.mapPartitions(load_rows_as_tables(client, edge_descriptor))
print(f"loaded edges: {edges.collect()}")

gdsarrow.send_action(client, "RELATIONSHIP_LOAD_DONE",
                     {"name": "test"}, NEO4J_HOST, 4242)

### Stop flight server
gds.run_cypher("CALL gds.alpha.flightServer.stop();")

### Check Graph is Real
# TBD
