from jpype.types import *
import jpype.imports
import pyarrow

from com.neo4j.gds.experimental.python import PyForreignAllocation
from com.neo4j.gds.experimental.python import ForreignBigIntVector
from com.neo4j.gds.experimental.python import ForreignFloat8Vector
from com.neo4j.gds.experimental.python import PyForreignAllocation
from org.apache.arrow.memory import RootAllocator


class JavaArrowHelper:

    def __init__(self) -> None:
        self._java_allocator = RootAllocator()

    def to_java_buffer(self, pbuffer):
        if pbuffer is None:
            return None
        return self._java_allocator.wrapForeignAllocation(PyForreignAllocation(pbuffer.size, pbuffer.address))

    def to_java_array(self, parray):
        jbuffers = list(map(lambda pbuffer: self.to_java_buffer(pbuffer), parray.buffers()))

        if parray.type.equals(pyarrow.int64()):
            return ForreignBigIntVector(jbuffers[0], jbuffers[1], len(parray))
        elif parray.type.equals(pyarrow.float64()):
            return ForreignFloat8Vector(jbuffers[0], jbuffers[1], len(parray))
        else:
            raise "Unsupported type"
