import os
import pytest

os.environ.setdefault(
    "JAVA_HOME",
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder.appName("macro-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
