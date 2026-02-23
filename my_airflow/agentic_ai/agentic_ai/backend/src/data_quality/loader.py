from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def loader_agent(state: dict) -> dict:
 
    state["df"] = spark.read.table(state['full_name'])

    return state







if __name__ == "__main__":
    state = {}

    state['tables_dq_check'] = ['12345'] 


    state = loader_agent(state)
    state["df"].show()

