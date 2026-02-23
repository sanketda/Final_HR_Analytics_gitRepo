import os
import requests
import pandas as pd
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}


def fetch_catalogs():
    url = f"{DATABRICKS_HOST}/api/2.1/unity-catalog/catalogs"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json().get("catalogs", [])


def fetch_schemas_by_catalog(catalog_name):
    url = f"{DATABRICKS_HOST}/api/2.1/unity-catalog/schemas"
    params = {"catalog_name": catalog_name}
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json().get("schemas", [])


def fetch_tables_by_catalog_schema(catalog_name, schema_name):
    url = f"{DATABRICKS_HOST}/api/2.1/unity-catalog/tables"
    params = {
        "catalog_name": catalog_name,
        "schema_name": schema_name
    }
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json().get("tables", [])


def main():
    all_tables = []

    # Fetch all catalogs
    catalogs = fetch_catalogs()

    for catalog in catalogs:
        catalog_name = catalog.get("name")

        schemas = fetch_schemas_by_catalog(catalog_name)
        for schema in schemas:
            schema_name = schema.get("name")

            tables = fetch_tables_by_catalog_schema(catalog_name, schema_name)
            for table in tables:
                table["catalog_name"] = catalog_name
                table["schema_name"] = schema_name

                # Convert 'columns' list to JSON string to keep it intact in CSV
                if 'columns' in table:
                    table["columns"] = json.dumps(table["columns"])

                # Optional: preserve other nested fields as JSON if desired
                for field in ['table_constraints', 'view_dependencies', 'delta_runtime_properties_kvpairs']:
                    if field in table:
                        table[field] = json.dumps(table[field])

                # Add custom field: default False
                table["last_dq_checked"] = False

                all_tables.append(table)

    # Define important top-level fields to include
    important_fields = [
        "catalog_name"  ,
        "schema_name"  ,
        "name"  ,
        "full_name"  ,
        "table_type"  ,
        "data_source_format"  ,
        "storage_location",
        "owner"  ,
        "created_at"  ,
        "created_by"  ,
        "updated_at"  ,
        "updated_by"  ,
        "last_dq_checked",
        "browse_only"  ,
        "columns" 
        
    ]

    # Flatten table metadata
    tables_df = pd.json_normalize(all_tables, sep='.')

    # Keep only important fields that exist
    existing_fields = [col for col in important_fields if col in tables_df.columns]
    tables_df = tables_df[existing_fields]

    # Save to CSV
    tables_df.to_csv("tables.csv", index=False)


if __name__ == "__main__":
    main()
