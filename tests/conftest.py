import pytest
from pytest import MonkeyPatch
from pyspark.sql import SparkSession
from pytest import FixtureRequest
import datetime as dt
import delta

from tests.fake_spark import FakeSpark
from spark_tests.datetime import FakeDatetime
from spark_tests.delta import FakeDeltaTable


@pytest.fixture(scope="session")
def spark_session(request: FixtureRequest) -> SparkSession:
    """Fixture for creating a `SparkSession`
    """
    spark = SparkSession.builder.getOrCreate()
    request.addfinalizer(lambda: spark.sparkContext.stop())
    return spark


@pytest.fixture(scope="session")
def fake_spark(spark_session: SparkSession) -> FakeSpark:
    return FakeSpark(spark_session)


@pytest.fixture(scope="function")
def mock_datetime(monkeypatch: MonkeyPatch) -> None:
    """Mocks dt.datetime
    """
    FakeDatetime.set_fake_now(dt.datetime(2021, 3, 20))
    monkeypatch.setattr(dt, "datetime", FakeDatetime)
    monkeypatch.setattr(dt, "date", FakeDatetime)
    return


@pytest.fixture(scope="function")
def mock_delta_table(monkeypatch: MonkeyPatch) -> None:
    """Mocks DeltaTable"""
    monkeypatch.setattr(delta, "DeltaTable", FakeDeltaTable)
