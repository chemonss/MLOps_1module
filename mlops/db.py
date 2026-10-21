import clickhouse_connect
from mlops.config import CH_HOST, CH_USER, CH_PASSWORD

def client():
    return clickhouse_connect.get_client(host=CH_HOST, username=CH_USER,
                                          password=CH_PASSWORD)
