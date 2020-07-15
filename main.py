import pandas as pd
import streamlit as st

from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data import (
    KustoClient,
    KustoConnectionStringBuilder,
    ClientRequestProperties,
)


def connect_kusto() -> KustoClient:
    # Configuration
    cluster = "https://help.kusto.windows.net"

    # Authentication
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)
    client = KustoClient(kcsb)
    return client


@st.cache
def query_kusto(db, text) -> pd.DataFrame:
    response = client.execute(db, text)

    # Transform to pandas df
    df = dataframe_from_result_table(response.primary_results[0])
    return df


def draw_interface():
    st.title("Learning Kusto")


if __name__ == "__main__":

    # Data
    client = connect_kusto()
    db = "Samples"

    query1 = """
    StormEvents
    |   take 10
    """

    df1 = query_kusto(db, query1)

    # Interface
    draw_interface()

    st.dataframe(df1)
