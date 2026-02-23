from langgraph.graph import StateGraph, END
from validation_spark import validation_agent
from database import dq_check_tables


def watcher_node(state: dict):

    state["tables"] = dq_check_tables() #list of table_ids

    return state




workflow = StateGraph(dict)

workflow.add_node("watcher", watcher_node) #identify tables to dq check


workflow.add_node("validation", validation_agent
                  )

workflow.set_entry_point("watcher")


workflow.add_edge("watcher", "validation")
workflow.add_edge("validation", END)

app = workflow.compile()


if __name__ == "__main__":
    results = app.invoke({})
    print(results)
