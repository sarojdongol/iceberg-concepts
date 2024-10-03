https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriterV2.html


1. pakcages: this package includes the class that spark needs to interact with iceberg tables and medata. Necesary class are added in the classpath
2. package version should be compatiable with scal and Apache Spark.


catalog: create my_catalog 

.config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \






Reference:
How to tracle orphan parquet?
https://www.tabular.io/apache-iceberg-cookbook/data-operations-orphan-file-cleanup/



Procedures:
https://iceberg.apache.org/docs/latest/spark-procedures/#output_1
