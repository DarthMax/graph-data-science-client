import jpype


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class JvmInstance(metaclass=Singleton):

    def __init__(self) -> None:
        jpype.startJVM("--add-opens=java.base/java.nio=ALL-UNNAMED", classpath=[
            "/home/max/coding/graph-analytics/private/continuous-benchmarking/build/libs/continuous-benchmarking.jar"])