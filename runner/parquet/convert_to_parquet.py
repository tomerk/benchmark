from pyspark import SparkContext
from pyspark.sql import HiveContext

sc = SparkContext(appName = "Parquet Converter")
hiveContext = HiveContext(sc)
hiveContext.setConf("spark.sql.parquet.compression.codec", "snappy")

hiveContext.sql("SET spark.sql.shuffle.partitions=2000")

hiveContext.table("rankings").repartition(20).saveAsParquetFile("/user/spark/benchmark/rankings-parquet")
hiveContext.table("documents").repartition(120).saveAsParquetFile("/user/spark/benchmark/documents-parquet")
hiveContext.sql("select * from uservisits order by visitdate").repartition(120).saveAsParquetFile("/user/spark/benchmark/uservisits-parquet")
