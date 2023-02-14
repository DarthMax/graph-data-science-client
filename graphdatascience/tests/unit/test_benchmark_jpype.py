from typing import Generator

import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.local.jvm_instance import JvmInstance
JvmInstance()

from jpype.types import *
import jpype.imports

from java.util import ArrayList

def fill_python_array_string():
    list = [None] * 10_000_000
    for x in range(0, 10_000_000):
        list.append(str(x))

def fill_java_array_string():
    list = ArrayList(10_000_000)
    for x in range(0, 10_000_000):
        list.add(str(x))

def fill_python_array_long():
    list = [None] * 10_000_000
    for x in range(0, 10_000_000):
        list.append(x)

def fill_java_array_long():
    array = JLong[10_000_000]
    for x in range(0, 10_000_000):
        array[x] = x

def test_python_fill_list_string(benchmark):
    benchmark(fill_python_array_string)

def test_java_fill_list_string(benchmark):
    benchmark(fill_java_array_string)

def test_python_fill_array_long(benchmark):
    benchmark(fill_python_array_long)

def test_java_fill_array_long(benchmark):
    benchmark(fill_java_array_long)