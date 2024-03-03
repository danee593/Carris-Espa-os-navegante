import os
import requests
import json
import pandas as pd
from datetime import datetime
from google.cloud import bigquery


TABLE_ID: str = os.environ.get("table_id")
SCHEMA = [bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
          bigquery.SchemaField("name", "STRING"),
          bigquery.SchemaField("lat", "STRING"),
          bigquery.SchemaField("lon", "STRING"),
          bigquery.SchemaField("phone", "STRING"),
          bigquery.SchemaField("email", "STRING"),
          bigquery.SchemaField("url", "STRING"),
          bigquery.SchemaField("address", "STRING"),
          bigquery.SchemaField("postal_code", "STRING"),
          bigquery.SchemaField("locality", "STRING"),
          bigquery.SchemaField("parish_id", "STRING"),
          bigquery.SchemaField("parish_name", "STRING"),
          bigquery.SchemaField("municipality_id", "STRING"),
          bigquery.SchemaField("municipality_name", "STRING"),
          bigquery.SchemaField("district_id", "STRING"),
          bigquery.SchemaField("district_name", "STRING"),
          bigquery.SchemaField("region_id", "STRING"),
          bigquery.SchemaField("region_name", "STRING"),
          bigquery.SchemaField("hours_monday", "STRING"),
          bigquery.SchemaField("hours_tuesday", "STRING"),
          bigquery.SchemaField("hours_wednesday", "STRING"),
          bigquery.SchemaField("hours_thursday", "STRING"),
          bigquery.SchemaField("hours_friday", "STRING"),
          bigquery.SchemaField("hours_saturday", "STRING"),
          bigquery.SchemaField("hours_sunday", "STRING"),
          bigquery.SchemaField("hours_special", "STRING"),
          bigquery.SchemaField("stops", "STRING"),
          bigquery.SchemaField("currently_waiting", "STRING"),
          bigquery.SchemaField("expected_wait_time", "STRING"),
          bigquery.SchemaField("active_counters", "STRING"),
          bigquery.SchemaField("is_open", "STRING"),
          bigquery.SchemaField("time", "STRING")]


def main(data, context):  # cloud functions require two arguments
    """
    This module queries the carris api: https://github.com/carrismetropolitana/api
    ,getting the encm dataset then it is inserted into a bigquery table.
    :param data: required for cloud function, doesn't interact with the script
    :param context: required for cloud function, doesn't interact with the script
    :return: returns 0 when executed correctly.
    """
    carris_encm: pd.DataFrame = get_carris_encm()
    client = bigquery.Client()
    load_to_bigquery(client=client, dataframe=carris_encm)
    client.close()
    return 0


def get_carris_encm() -> pd.DataFrame:
    """
    Function to get from the carris api the encm dataset.
    Convert to a dataframe object, add a timestamp and return to the user
    :return: a pandas dataframe that contains the encm dataset.
    :raises: SystemExit when a request error occurred.
    """
    carris_encm_url = "https://api.carrismetropolitana.pt/datasets/facilities/encm"
    try:
        response = requests.get(url=carris_encm_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise SystemExit(f"Failed to fetch data from API: {str(e)}")
    else:
        response_time: str = datetime.now().strftime("%y/%m/%d %H:%M:%S")
        response_json: dict = json.loads(response.text)
        df = pd.DataFrame(response_json)
        df["time"] = response_time
        df = df.astype(str)
        return df


def load_to_bigquery(client, dataframe):
    """
    function that loads the dataframe to bigquery
    :param client: a client connection object to bigquery
    :param dataframe: a carris_encm dataframe
    :return: None, prints to cloud console the number of rows inserted.
    """
    job_config = bigquery.LoadJobConfig(schema=SCHEMA)
    job = client.load_table_from_dataframe(dataframe,
                                           TABLE_ID,
                                           job_config=job_config)
    job.result()
    table = client.get_table(TABLE_ID)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), TABLE_ID
        )
    )


if __name__ == "__main__":
    main()
