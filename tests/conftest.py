import os
import sys
import pytest

# ml/ là nơi các script sống + chạy (sys.path có script dir); test import sibling
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ml"))

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
