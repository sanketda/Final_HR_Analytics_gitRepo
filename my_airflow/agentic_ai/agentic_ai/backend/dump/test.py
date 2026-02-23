from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from database import get_full_name,get_metadata

spark = SparkSession.builder.appName("DataQualityValidation").getOrCreate()

def loader(state: dict) -> dict:
    print(get_full_name(state["table_id"]))
    print(get_metadata(state["table_id"]))
    state["full_name"] = get_full_name(state["table_id"])
    state["columns_metadata_json"] = get_metadata(state["table_id"])
    state["df"] =  spark.read.table(state["full_name"])
    
    state["expected_schema"] = {col["name"]: col["type_name"] for col in state["columns_metadata_json"]}
    state["expected_columns"] = [col["name"] for col in state["columns_metadata_json"]]
    state["nullable_map"]    = {col["name"]: col["nullable"]  for col in state["columns_metadata_json"]}
    print(state["nullable_map"])
    print(state["expected_columns"])
    df = spark.read.table(state["full_name"])
    df.show()
    return state


if __name__ == "__main__":
    state ={}
    state["table_id"] = "338c21da-10c1-459f-85bf-9251c30dd9b8"

    loader(state)
