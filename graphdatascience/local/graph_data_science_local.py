from typing import TypeVar, List, Dict, Any

import jpype
import numpy as np
import pandas
import pyarrow
from jpype import JImplements, JOverride
from pandas import DataFrame, Series
from jpype.types import *

jpype.startJVM("--add-opens=java.base/java.nio=ALL-UNNAMED", classpath=["/home/max/coding/work/graph-analytics/private/continuous-benchmarking/build/libs/continuous-benchmarking.jar"])

import jpype.imports
from org.neo4j.gds.core.loading.construction import GraphFactory
from org.neo4j.gds import Orientation
from org.neo4j.gds.core.loading import RelationshipImportResult
from java.util import Map
from org.neo4j.gds.core.loading import GraphStoreBuilder
from org.neo4j.gds.core.loading import CSRGraphStoreUtil
from org.neo4j.gds.api import DatabaseId
from java.util import Optional
from com.neo4j.gds.experimental.python import AlgoRunner
from com.neo4j.gds.experimental.python import PyForreignAllocation
from org.neo4j.gds.core.loading import GraphStoreCatalog
from org.neo4j.gds.config import GraphProjectFromStoreConfig
from org.neo4j.gds.api import RelationshipConsumer
from org.apache.arrow.memory import RootAllocator
from org.apache.arrow.vector import BigIntVector
from org.apache.arrow.vector.ipc.message import ArrowFieldNode
from java.util import ArrayList

class LocalGDS:
    # def __init__(
    #     self,
    #     class_path: str,
    # ):

    def construct(self, graph_name: str, node_df: DataFrame, relationship_df: DataFrame):
        node_count = node_df.shape[0]
        nodes_builder = GraphFactory.initNodesBuilder()\
            .maxOriginalId(node_count).\
            nodeCount(node_count).\
            concurrency(4).build()

        for index, row in node_df.iterrows():
            nodes_builder.addNode(row['nodeId'])

        nodes = nodes_builder.build()

        relationships_builder = GraphFactory.initRelationshipsBuilder()\
            .nodes(nodes.idMap())\
            .orientation(Orientation.NATURAL)\
            .concurrency(4)\
            .build()

        for index, row in relationship_df.iterrows():
            relationships_builder.add(row['sourceId'], row['targetId'])

        relationships = relationships_builder.build()

        graph = GraphFactory.create(nodes.idMap(), relationships)

        graph_store = CSRGraphStoreUtil.createFromGraph(DatabaseId.fromString("gdl"), graph, "REL", Optional.empty(), 4)

        GraphStoreCatalog.set(
            GraphProjectFromStoreConfig.emptyWithName("", graph_name),
            graph_store
        )

        return Graph(graph_store, graph_name)


class Graph:
    def __init__(self, graph_store, graph_name):
        self._graph_store = graph_store
        self._graph_name = graph_name

    def node_count(self):
        return self._graph_store.nodeCount()

    def relationship_count(self):
        return self._graph_store.relationshipCount()

    def for_each_node(self, node_consumer):
        self._graph_store.getUnion().forEachNode(lambda x: node_consumer(x))

    def for_each_relationship(self, node_id, relationship_consumer):

        @JImplements(RelationshipConsumer)
        class _RelationshipConsumer(object):
            @JOverride
            def accept(self, source, target):
                return relationship_consumer(source, target)

        self._graph_store.getUnion().forEachRelationship(node_id, _RelationshipConsumer())

    def node_properties(self, property_key):
        values = self._graph_store.nodeProperty(property_key).values()

        nodeIds = range(0, self.node_count())
        propertValues = map(lambda node: values.getObject(node), nodeIds)

        return DataFrame({"nodeId": nodeIds, property_key: propertValues})

    def run_algorithm(self, algorithm: str, config: Dict[str, Any]):
        java_result = AlgoRunner.run(self._graph_name, algorithm, config)

        def map_keys_to_string(row):
            return dict((str(name), row[name]) for name in row.keySet())

        python_result = list(map(map_keys_to_string, java_result))

        return pandas.DataFrame.from_records(python_result)


def gds_example():
    gds = LocalGDS()
    nodes = DataFrame({'nodeId': range(0, 100)})
    relationships = DataFrame(np.random.randint(0, 100, size=(100, 2)), columns=["sourceId", "targetId"])

    try:
        graph = gds.construct("graph", nodes, relationships)
        print(f"Node count: {graph.node_count()}")
        print(f"Relationship count: {graph.relationship_count()}")

        def print_rels(source, target):
            print(f'({source})-->({target})')
            return True

        def print_nodes(node):
            graph.for_each_relationship(node, print_rels)
            return True

        graph.for_each_node(print_nodes)

        result = graph.run_algorithm("gds.wcc.mutate", {"mutateProperty": "prop", "sudo": True})
        print(result)

        print(graph.node_properties("prop"))

    except jpype.JException as exception:
        print(exception.message())
        print(exception.stacktrace())


def pbuffer_to_java(python_buffer, java_allocator):
    if python_buffer is None:
        return None

    return java_allocator.wrapForeignAllocation(PyForreignAllocation(python_buffer.size, python_buffer.address))


def parray_to_java(python_arrow_array, jallocator):
    jbuffers = list(map(lambda buffer: pbuffer_to_java(buffer, jallocator), python_arrow_array.buffers()))

    jarray = BigIntVector("from_python", jallocator)
    jarray.loadFieldBuffers(ArrowFieldNode(100, python_arrow_array.null_count), ArrayList(jbuffers))

    # Close the wrapped java buffers as they are no longer needed
    for b in jbuffers:
        if b is not None:
            b.close()

    return jarray


def arrow_example():
    try:
        parray = pyarrow.array(Series(range(0, 100)))

        jallocator = RootAllocator()
        jarray = parray_to_java(parray, jallocator)
        print(jarray.get(42))

        jarray.close()
        jallocator.close()
    except jpype.JException as exception:
        print(exception.message())
        print(exception.stacktrace())


if __name__ == "__main__":
    # main()
    arrow_example()