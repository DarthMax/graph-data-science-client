import jpype
import numpy as np
from pandas import DataFrame

from graphdatascience.local.graph_data_science_local import LocalGDS

def gds_example():
    gds = LocalGDS()
    nodes = DataFrame({'nodeId': range(0, 100)})
    relationships = DataFrame(np.random.randint(0, 100, size=(100, 2)), columns=["sourceId", "targetId"])

    try:
        graph = gds.construct_jni("graph", nodes, relationships)
        print(f"Node count: {graph.node_count()}")
        print(f"Relationship count: {graph.relationship_count()}")

        def print_rels(source, target):
            print(f'({source})-->({target})')
            return True

        def print_nodes(node):
            graph.for_each_relationship(node, print_rels)
            return True

        graph.for_each_node(print_nodes)

        print("###### Mutate Result")
        result = graph.run_algorithm("gds.wcc.mutate", {"mutateProperty": "community", "sudo": True})
        print(result.to_markdown())

        print("###### Get mutated property")
        print(graph.node_properties("community").to_markdown())

    except jpype.JException as exception:
        print(exception.message())
        print(exception.stacktrace())


if __name__ == "__main__":
    gds_example()