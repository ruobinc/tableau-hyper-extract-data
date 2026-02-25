"""
Extract data from a Tableau .tdsx file via Tableau Server Client,
then read and print data from the embedded Hyper file.
"""

import os
import shutil
import zipfile
from pathlib import Path

import tableauserverclient as TSC
from dotenv import load_dotenv
from tableauhyperapi import Connection, HyperProcess, TableName, Telemetry

# Load environment variables from .env
load_dotenv()

SERVER_NAME = os.getenv("SERVER_NAME")
SITE_NAME = os.getenv("SITE_NAME")
PAT_NAME = os.getenv("PAT_NAME")
PAT_VALUE = os.getenv("PAT_VALUE")
DATASOURCE_ID = os.getenv("DATASOURCE_ID")
OUTPUT_PATH = "./"


# ---------------------------------------------------------------------------
# Step 1: Download .tdsx from Tableau Server
# ---------------------------------------------------------------------------
def download_tdsx() -> str:
    """Authenticate with Tableau Server and download the datasource as .tdsx."""
    tableau_auth = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_VALUE, site_id=SITE_NAME)
    server = TSC.Server(SERVER_NAME, use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        file_path = server.datasources.download(
            DATASOURCE_ID,
            filepath=OUTPUT_PATH,
            include_extract=True,
        )
    print(f"Downloaded: {file_path}")
    return file_path


# ---------------------------------------------------------------------------
# Step 2: Extract .hyper file from .tdsx (zip archive)
# ---------------------------------------------------------------------------
def extract_hyper(tdsx_path: str) -> str:
    """Extract the first .hyper file found inside the .tdsx archive."""
    with zipfile.ZipFile(tdsx_path, "r") as z:
        for name in z.namelist():
            if name.endswith(".hyper"):
                z.extract(name, "./")
                hyper_path = os.path.join("./", name)
                print(f"Extracted: {hyper_path}")
                return hyper_path
    raise FileNotFoundError("No .hyper file found inside the .tdsx archive.")


# ---------------------------------------------------------------------------
# Step 3: Read and print data from the Hyper file
# ---------------------------------------------------------------------------
def read_hyper(hyper_path: str) -> None:
    """Print schema and all rows from the Hyper file."""
    # Copy the hyper file so the original is not modified
    path_to_database = Path(
        shutil.copy(src=hyper_path, dst="superstore_sample_denormalized_read.hyper")
    ).resolve()

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=path_to_database) as conn:
            # List all tables in the "Extract" schema
            table_names = conn.catalog.get_table_names(schema="Extract")

            for table in table_names:
                table_def = conn.catalog.get_table_definition(name=table)
                print(f"\nTable: {table.name} (qualified: {table})")
                for column in table_def.columns:
                    print(
                        f"  Column {column.name}: type={column.type}, "
                        f"nullability={column.nullability}"
                    )

            # Print all rows from the last table
            table_name = TableName(table_names[-1].schema_name, table_names[-1].name)
            print(f"\nAll rows in {table_name}:")
            rows = conn.execute_list_query(query=f"SELECT * FROM {table_name}")
            for row in rows:
                print(row)

        print("\nConnection closed.")
    print("Hyper process shut down.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    tdsx_path = download_tdsx()
    hyper_path = extract_hyper(tdsx_path)
    read_hyper(hyper_path)
