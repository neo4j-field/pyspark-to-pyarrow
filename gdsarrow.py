import pyarrow as pa
import pyarrow.flight as flight

import json
from time import sleep

def send_action(client: flight.FlightClient, action_type: str, meta_data: dict) -> dict:
    try:
        result = client.do_action(
            flight.Action(action_type, json.dumps(meta_data).encode("utf-8"))
        )
        obj = json.loads(next(result).body.to_pybytes().decode())
        print(f"{action_type} result")
        print(json.dumps(obj, indent=4))
        return obj
    except Exception as e:
        print(f"send_action error: {e}")
        return e

def write_table(client: flight.FlightClient, desc: bytes, table: pa.Table) -> int:
    """
    Write a PyArrow Table to the GDS Flight service.
    """
    # Write schema
    upload_descriptor = flight.FlightDescriptor.for_command(desc)
    writer, meta_data_reader = client.do_put(upload_descriptor, table.schema)

    writer, _ = client.do_put(upload_descriptor, table.schema)
    rows = len(table)
    with writer:
        try:
            writer.write_table(table, max_chunksize=10_000)
            return rows
        except Exception as e:
            print(e)
    return -1
