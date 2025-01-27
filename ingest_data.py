#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def download_file(url, output_path):
    """Download file from the URL and save it to the specified path."""
    try:
        logging.info(f"Downloading file from {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise HTTPError for bad responses
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"File downloaded successfully to {output_path}")
    except Exception as e:
        logging.error(f"Failed to download file: {e}")
        raise

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # Derive file name from URL
    csv_name = url.split("/")[-1]

    # Download the file
    try:
        download_file(url, csv_name)
    except Exception as e:
        logging.error("Exiting due to download error.")
        return

    # Connect to PostgreSQL database
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        return

    # Read CSV in chunks
    try:
        df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

        # Process the first chunk
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        # Create table
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        logging.info(f"Table {table_name} created successfully.")

        # Insert first chunk
        df.to_sql(name=table_name, con=engine, if_exists='append')
        logging.info("First chunk inserted successfully.")

        # Process remaining chunks
        while True:
            try:
                t_start = time()
                df = next(df_iter)

                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

                df.to_sql(name=table_name, con=engine, if_exists='append')

                t_end = time()
                logging.info(f"Inserted another chunk, took {t_end - t_start:.3f} seconds")

            except StopIteration:
                logging.info("Finished ingesting data into the PostgreSQL database")
                break

    except Exception as e:
        logging.error(f"Error processing data: {e}")
    finally:
        # Cleanup
        if os.path.exists(csv_name):
            os.remove(csv_name)
            logging.info(f"Temporary file {csv_name} deleted.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='User name for Postgres')
    parser.add_argument('--password', required=True, help='Password for Postgres')
    parser.add_argument('--host', required=True, help='Host for Postgres')
    parser.add_argument('--port', required=True, help='Port for Postgres')
    parser.add_argument('--db', required=True, help='Database name for Postgres')
    parser.add_argument('--table_name', required=True, help='Name of the table to write data to')
    parser.add_argument('--url', required=True, help='URL of the CSV file')

    args = parser.parse_args()

    main(args)
