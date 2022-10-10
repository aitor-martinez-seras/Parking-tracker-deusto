from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from utils.constants import TOKEN, ORG, BUCKET


def data_to_influx(now: datetime, general_number: int, dbs_number: int):
    """
    Function that connects to influxdb and writes the collected data point
    :param now: the timestamp of the data
    :param general_number: the free spaces in the general parking
    :param dbs_number: the free spaces in the DBS parking
    :return:
    """
    # Connection. The URL is from my local network
    client = InfluxDBClient(url="http://192.168.1.90:8086", token=TOKEN)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Create the data points using the Point class from the influxdb package
    point_general = Point("free_spaces")\
        .tag("Parking", "General")\
        .field("free_spaces", general_number)\
        .time(now, WritePrecision.NS)
    point_dbs = Point("free_spaces") \
        .tag("Parking", "DBS") \
        .field("free_spaces", dbs_number) \
        .time(now, WritePrecision.S)
    # Insert them into a list to write both data points at the same time
    sequence = [point_general,
                point_dbs]
    # Write the data points
    write_api.write(BUCKET, ORG, sequence)


def query_data_from_influx():
    # Connection. The URL is from my local network
    client = InfluxDBClient(url="http://192.168.1.90:8086", token=TOKEN)
    print('hello')
    query = f'from(bucket: "{BUCKET}") |> range(start: -5m) |> filter(fn: (r) => r.Parking == "DBS)'
    #query = f'from(bucket: "{BUCKET}") |> range(start: -5m)'
    tables = client.query_api().query(query, org=ORG)
    return tables


if __name__ == "__main__":
    tables = query_data_from_influx()



