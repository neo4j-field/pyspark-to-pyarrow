import pyarrow as pa
import pyarrow.flight as flight

import json
from time import sleep

def send_action(client: flight.FlightClient, action_type: str, meta_data: dict,
                arrow_host: str, arrow_port: int) -> dict:
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

def write_table(client: flight.FlightClient, desc: flight.FlightDescriptor,
                table: pa.Table) -> int:
    """
    Write a PyArrow Table to the GDS Flight service.
    Has some retry logic (overkill!).
    """
    # Write schema
    upload_descriptor = flight.FlightDescriptor.for_command(desc)
    writer, meta_data_reader = client.do_put(upload_descriptor, table.schema)

    # set up a mutable queue of retries
    max_retries = 10
    remaining_retries = list(range(0, max_retries))
    remaining_retries.reverse() # we need to pop() items out in reverse

    with writer:
        while remaining_retries:
            try:
                writer.write_table(table, max_chunksize=self.chunk_size)
                break
            except flight.FlightUnavailableError as e:
                if not remaining_retries:
                    raise e
                # a simple exponential backoff
            secs = 1 << remaining_retries.pop()
            print(f'FlightUnavailableError: backing off for {secs} seconds')
            sleep(secs)
            times_retried = max_retries - len(remaining_retries)
            return times_retried
