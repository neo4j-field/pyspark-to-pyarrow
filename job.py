from typing import Iterator, Tuple
import os
import sys
import json
import uuid

import pyspark
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import pandas_udf

import pyarrow as pa
import pyarrow.flight as flight
from graphdatascience import GraphDataScience

import gdsarrow # local


def load_rows_as_tables(arrow_host: str, arrow_port: int, desc: dict):
    """
    Higher-order function that converts PySpark Rows into PyArrow Tables and
    feeds them to Neo4j.
    """
    # todo: move out
    desc = json.dumps(desc).encode("utf-8")

    def loader(iterator: Iterator[Row]):
        client = flight.FlightClient(
            flight.Location.for_grpc_tcp(arrow_host, arrow_port)
        )
        # xxx this is a hack and will not scale
        data = dict()
        for row in iterator:
            for field in row.asDict():
                col = data.get(field, [])
                col.append(row[field])
                data[field] = col
        table = pa.table(data)
        yield str(table.schema), len(table), gdsarrow.write_table(client, desc, table)
        #yield len(table), str(table.schema), data.get("source_id", [0])[0], data.get("destination_id", [0])[0]
    return loader

def guid_to_int(*fields):
    """Convert fields in a given row from a str-based guid to an int value"""
    def _guid_to_int(row: Row) -> Row:
        # this is a total hack...mask off the upper 65 bits :(
        result = {}
        for field in row.asDict():
            if field in fields:
                guid = uuid.UUID(str(row[field]))
                result[field] = int(int(guid) & ((1 << 63) - 1)) # need a sign bit :(
            else:
                result[field] = row[field]
        return Row(**result)
    return _guid_to_int


### Set up Neo4j stuff
if len(sys.argv) < 4:
    print("uhhh see usage?")
    os.exit(1)

_, NEO4J_HOST, NEO4J_USER, NEO4J_PASS = sys.argv[:4]
gds = GraphDataScience(f"bolt://{NEO4J_HOST}:7687", auth=(NEO4J_USER, NEO4J_PASS))
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
    .withColumnRenamed("guid", "node_id")
    .cache()
)
edges =  (
    spark.read
    .format("bigquery")
    .load("neo4j-se-team-201905.fraud_demo_data.p2p")
    .repartition(8)
    .withColumnRenamed("start_guid", "source_id")
    .withColumnRenamed("end_guid", "destination_id")
    .drop("start_label", "end_label", "transactionDateTime", "totalAmount")
    .cache()
)
print(f">>> Starting with {nodes.count():,} nodes, {edges.count():,} edges")
print(f">>> nodes: {nodes.dtypes}")
print(f">>> edges: {edges.dtypes}")


### Start flight server & declare we're creating a GDS graph
gds.run_cypher("CALL gds.alpha.flightServer.start({ abortionTimeout: 900000000 });")

client = flight.FlightClient(flight.Location.for_grpc_tcp(NEO4J_HOST, 4242))
res = gdsarrow.send_action(client, "CREATE_GRAPH", {"name": "test"})
print(res)

### Load Nodes -- remap our node id column
node_descriptor = { "name": "test", "entity_type": "node" }
nodes = (
    nodes.rdd
    .map(guid_to_int("node_id"), True)
    .mapPartitions(load_rows_as_tables(NEO4J_HOST, 4242, node_descriptor), True)
)
print(f"loaded nodes: {nodes.collect()}")

gdsarrow.send_action(client, "NODE_LOAD_DONE", {"name": "test"})

### Load Edges -- we need to remap some columns here as well
edge_descriptor = { "name": "test", "entity_type": "relationship" }
edges = (
    edges.rdd
    .map(guid_to_int("source_id", "destination_id"), True)
    .mapPartitions(load_rows_as_tables(NEO4J_HOST, 4242, edge_descriptor))
)
print(f"loaded edges: {edges.collect()}")

gdsarrow.send_action(client, "RELATIONSHIP_LOAD_DONE", {"name": "test"})

### Stop flight server
gds.run_cypher("CALL gds.alpha.flightServer.stop();")

### Check Graph is Reaol
# TBD
