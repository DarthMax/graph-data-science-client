from typing import Dict, Any

import pandas
import pyarrow
from jpype import JImplements, JOverride
from pandas import DataFrame, Series

from graphdatascience.local.jvm_instance import JvmInstance
JvmInstance()
from graphdatascience.local.java_arrow_integration import JavaArrowHelper

from org.neo4j.gds.core.loading.construction import GraphFactory
from org.neo4j.gds import Orientation
from org.neo4j.gds.core.loading import CSRGraphStoreUtil
from org.neo4j.gds.api import DatabaseId
from java.util import Optional
from com.neo4j.gds.experimental.python import AlgoRunner
from org.neo4j.gds.core.loading import GraphStoreCatalog
from org.neo4j.gds.config import GraphProjectFromStoreConfig
from org.neo4j.gds.api import RelationshipConsumer
from org.apache.arrow.memory import RootAllocator
from com.neo4j.gds.experimental.python import PythonGraphImporter
from org.neo4j.gds import RelationshipType

class LocalGDS:

    def __init__(self) -> None:

        self._arrow_helper = JavaArrowHelper()

    def construct_jni(self, graph_name: str, node_df: DataFrame, relationship_df: DataFrame):
        node_count = node_df.shape[0]
        nodes_builder = GraphFactory.initNodesBuilder() \
            .maxOriginalId(node_count). \
            nodeCount(node_count). \
            concurrency(4).build()

        for index, row in node_df.iterrows():
            nodes_builder.addNode(row['nodeId'])

        nodes = nodes_builder.build()

        relationships_builder = GraphFactory.initRelationshipsBuilder() \
            .nodes(nodes.idMap()) \
            .relationshipType(RelationshipType.of("IS_CONNECTED")) \
            .orientation(Orientation.NATURAL) \
            .concurrency(4) \
            .build()

        for index, row in relationship_df.iterrows():
            relationships_builder.add(row['sourceId'], row['targetId'])

        relationships = relationships_builder.build()

        graph = GraphFactory.create(nodes.idMap(), relationships)

        graph_store = CSRGraphStoreUtil.createFromGraph(DatabaseId.fromString("gdl"), graph, Optional.empty(), 4)

        GraphStoreCatalog.set(
            GraphProjectFromStoreConfig.emptyWithName("", graph_name),
            graph_store
        )

        return Graph(graph_store, graph_name)

    def construct_arrow(self, graph_name: str, node_df: DataFrame, relationship_df: DataFrame):
        jallocator = RootAllocator()

        node_count = node_df.shape[0]
        node_importer = PythonGraphImporter.nodeImporter(node_count)

        node_java_array = self._arrow_helper.to_java_array(pyarrow.Array.from_pandas(Series(node_df.nodeId)))
        node_importer.add(node_java_array)
        node_java_array.close()

        nodes = node_importer.build()

        relationship_importer = PythonGraphImporter.relationshipImporter(nodes)

        pyarrow_source = pyarrow.Array.from_pandas(relationship_df.sourceId)
        pyarrow_target = pyarrow.Array.from_pandas(relationship_df.targetId)

        source_java_array = self._arrow_helper.to_java_array(pyarrow_source)
        target_java_array = self._arrow_helper.to_java_array(pyarrow_target)

        relationship_importer.add(source_java_array, target_java_array)
        source_java_array.close()
        target_java_array.close()

        relationships = relationship_importer.build()

        graph = GraphFactory.create(nodes.idMap(), relationships)

        graph_store = CSRGraphStoreUtil.createFromGraph(DatabaseId.fromString("gdl"), graph, "REL", Optional.empty(), 4)

        GraphStoreCatalog.set(
            GraphProjectFromStoreConfig.emptyWithName("", graph_name),
            graph_store
        )
        jallocator.close()
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

        node_ids = range(0, self.node_count())
        property_values = map(lambda node: values.getObject(node), node_ids)

        return DataFrame({"nodeId": node_ids, property_key: property_values})

    def run_algorithm(self, algorithm: str, config: Dict[str, Any]):
        java_result = AlgoRunner.run(self._graph_name, algorithm, config)

        def map_keys_to_string(row):
            return dict((str(name), row[name]) for name in row.keySet())

        python_result = list(map(map_keys_to_string, java_result))

        return pandas.DataFrame.from_records(python_result)
