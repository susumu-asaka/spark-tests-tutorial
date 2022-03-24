""" Define a subclass of `FakeSparkSession` with dummy test data.
"""
import pandas as pd

from spark_tests.sql import FakeSparkSession, FakeDataFrame


class FakeSpark(FakeSparkSession):
    """Proxy of class `SparkSession`

    Substitute memory data for Hive access.
    """

    def table(self, table_name: str) -> FakeDataFrame:
        """Returns test table specified by `table_name` as a `FakeDataFrame`.

.. csv-table:: my_db.health_tracker_bronze
   :file: test_db/my_db.health_tracker_bronze.csv
   :widths: auto
   :header-rows: 1

        **End of test database**

        Args:
            table_name: Nome of the table on the metastore.
        """
        if table_name == "my_db.health_tracker_bronze":
            pdf = pd.read_csv(f"tests/test_db/{table_name}.csv",
                              parse_dates=[2])
            schema = "value STRING, status STRING, p_event_date DATE"
            df = self.createDataFrame(pdf, schema)
            return df

        else:
            raise ValueError(f"Table not found: {table_name}")
