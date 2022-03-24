from pyspark.sql import Row
import datetime as dt

from spark_tests.delta import FAKE_DELTA_MERGE

import operations
from spark_tests.sql import FakeSparkSession, FAKE_DF_WRITER

db_name = "my_db"
bronze_table_name = f"{db_name}.health_tracker_bronze"
silver_table_name = f"{db_name}.health_tracker_silver"


def test_write_silver_table(fake_spark: FakeSparkSession,
                            mock_datetime: None,
                            mock_delta_table: None) -> None:
    FAKE_DF_WRITER.clear()
    FAKE_DELTA_MERGE.clear()

    operations.write_silver_table(fake_spark)

    assert FAKE_DF_WRITER.name == "my_db.health_tracker_silver"
    assert FAKE_DF_WRITER.save_format == "delta"
    assert FAKE_DF_WRITER.save_mode == "append"
    assert FAKE_DF_WRITER.partition_by == ("p_ingest_date",)
    assert FAKE_DF_WRITER.is_saved
    silver_rows = [Row(eventtime=dt.datetime(2021, 3, 19, 6),
                       name="Armando Clemente", steps=1245,
                       p_ingest_date=dt.date(2021, 3, 20)),
                   Row(eventtime=dt.datetime(2021, 3, 19, 6),
                       name="Meallan O'Conarain", steps=510,
                       p_ingest_date=dt.date(2021, 3, 20))]
    assert FAKE_DF_WRITER.source.collect() == silver_rows

    assert FAKE_DELTA_MERGE.target.name == bronze_table_name
    assert FAKE_DELTA_MERGE.target.alias_name == "tgt"
    assert FAKE_DELTA_MERGE.source.alias_name == "src"
    bronze_rows = [Row(value='{"time": "2021-03-19 06:00:00", '
                             '"name": "Armando Clemente", "steps": 1245}',
                       status="new", p_event_date=dt.date(2021, 3, 19)),
                   Row(value='{"time": "2021-03-19 06:00:00", '
                             '"name": "Meallan O\'Conarain", "steps": 510}',
                       status="new", p_event_date=dt.date(2021, 3, 19))]
    assert FAKE_DELTA_MERGE.source.collect() == bronze_rows
    assert FAKE_DELTA_MERGE.condition == "tgt.value = src.value"
    assert FAKE_DELTA_MERGE.matched_action == "UPDATE"
    assert FAKE_DELTA_MERGE.matched_update == {"status": "'loaded'"}
    assert FAKE_DELTA_MERGE.is_executed
