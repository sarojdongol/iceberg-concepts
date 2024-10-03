from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from utlis import cal_lat_log_dist
import fire


class DataProcesserPipline:
    def __init__(self, sparkContext):
        self._spark = sparkContext

    def _execute_ddl(self):
        self._spark.sql("CREATE DATABASE IF NOT EXISTS local.taxi_db")
        self._spark.sql("""CREATE TABLE IF NOT EXISTS local.taxi_db.taxi_nyc_br (
                                VendorID string,
                                tpep_pickup_datetime string,
                                tpep_dropoff_datetime string,
                                passenger_count string,
                                trip_distance string,
                                pickup_longitude string,
                                pickup_latitude string,
                                RateCodeID string,
                                store_and_fwd_flag string,
                                dropoff_longitude string,
                                dropoff_latitude string,
                                payment_type string,
                                fare_amount string,
                                extra string,
                                mta_tax string,
                                tip_amount string,
                                tolls_amount string,
                                improvement_surcharge string,
                                total_amount string
                              ) USING iceberg
                               """)

        self._spark.sql("""CREATE TABLE IF NOT EXISTS local.taxi_db.taxi_nyc_sil (
                  VendorID string,
                  tpep_pickup_datetime timestamp,
                  tpep_dropoff_datetime timestamp,
                  pickup_latitude string,
                  pickup_longitude string,
                  dropoff_latitude string,
                  dropoff_longitude string
                ) USING iceberg
                  PARTITIONED BY (day(tpep_pickup_datetime))
                ;""")

        self._spark.sql("""CREATE TABLE  IF NOT EXISTS local.taxi_db.taxi_nyc_gld (
                         VendorID string,
                         avg_trip_duration double
                         ) USING iceberg
                         ;""")
    
    def _execute_insert(self):
          self._spark.sql("""INSERT INTO local.taxi_db.taxi_nyc_br (
                              VendorID,
                              tpep_pickup_datetime,
                              tpep_dropoff_datetime,
                              passenger_count,
                              trip_distance,
                              pickup_longitude,
                              pickup_latitude,
                              RateCodeID,
                              store_and_fwd_flag,
                              dropoff_longitude,
                              dropoff_latitude,
                              payment_type,
                              fare_amount,
                              extra,
                              mta_tax,
                              tip_amount,
                              tolls_amount,
                              improvement_surcharge,
                              total_amount
                          ) VALUES
                  ('2', '2024-09-02 01:00:00', '2024-09-02 01:15:00', '2', '2.0', '-73.9751', '40.7780', '1', 'N', '-73.9651', '40.7880', '2', '10.0', '1.0', '0.5', '2.0', '0.0', '0.3', '13.8'),
                  ('1', '2024-09-02 02:00:00', '2024-09-02 02:10:00', '1', '1.5', '-73.9651', '40.7880', '1', 'N', '-73.9551', '40.7980', '1', '8.0', '0.5', '0.5', '1.5', '0.0', '0.3', '10.8'),
                  ('2', '2024-09-02 03:00:00', '2024-09-02 03:15:00', '3', '3.0', '-73.9551', '40.7980', '1', 'N', '-73.9451', '40.8080', '2', '15.0', '1.0', '0.5', '3.0', '0.0', '0.3', '19.8'),
                  ('1', '2024-09-02 04:00:00', '2024-09-02 04:10:00', '1', '1.0', '-73.9451', '40.8080', '1', 'N', '-73.9351', '40.8180', '1', '6.0', '0.5', '0.5', '1.0', '0.0', '0.3', '8.8'),
                  ('2', '2024-09-02 05:00:00', '2024-09-02 05:15:00', '2', '2.5', '-73.9351', '40.8180', '1', 'N', '-73.9251', '40.8280', '2', '12.0', '1.0', '0.5', '2.5', '0.0', '0.3', '16.8'),
                  ('1', '2024-09-02 06:00:00', '2024-09-02 06:10:00', '1', '1.5', '-73.9251', '40.8280', '1', 'N', '-73.9151', '40.8380', '1', '8.5', '0.5', '0.5', '1.5', '0.0', '0.3', '11.3'),
                  ('2', '2024-09-02 07:00:00', '2024-09-02 07:15:00', '3', '3.5', '-73.9151', '40.8380', '1', 'N', '-73.9051', '40.8480', '2', '17.0', '1.0', '0.5', '3.5', '0.0', '0.3', '22.3'),
                  ('1', '2024-09-02 08:00:00', '2024-09-02 08:10:00', '1', '1.0', '-73.9051', '40.8480', '1', 'N', '-73.8951', '40.8580', '1', '6.5', '0.5', '0.5', '1.0', '0.0', '0.3', '9.3'),
                  ('2', '2024-09-02 09:00:00', '2024-09-02 09:15:00', '2', '2.0', '-73.8951', '40.8580', '1', 'N', '-73.8851', '40.8680', '2', '11.0', '1.0', '0.5', '2.0', '0.0', '0.3', '15.8'),
                  ('1', '2024-09-02 10:00:00', '2024-09-02 10:10:00', '1', '1.5', '-73.8851', '40.8680', '1', 'N', '-73.8751', '40.8780', '1', '8.0', '0.5', '0.5', '1.5', '0.0', '0.3', '10.8');
                          """)
    
    def _execute_delete(self):
        self._spark.sql("""DELETE FROM local.taxi_db.taxi_nyc_br WHERE VendorID == '1';
""")
    
    def _execute_merge(self):
        pass
    

    def bronze_builder(self, dataframe):
        dataframe.writeTo("local.taxi_db.taxi_nyc_br").using("iceberg").append()
        dataframe.show()
        dataframe.printSchema()
        return dataframe

    def silver_builder(self, dataframe):
        dataframe = dataframe.select(
            F.col("VendorID").cast("string"),
            F.to_timestamp(F.col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
            .alias("tpep_pickup_datetime")
            .cast("timestamp"),
            F.col("tpep_dropoff_datetime").cast("timestamp"),
            F.col("pickup_latitude"),
            F.col("pickup_longitude"),
            F.col("dropoff_latitude"),
            F.col("dropoff_longitude"),
        )
        dataframe.writeTo("local.taxi_db.taxi_nyc_sil").using("iceberg").append()
        return dataframe

    def gold_builder(self, dataframe):
        df_gold = cal_lat_log_dist(
            dataframe,
            "pickup_latitude",
            "pickup_longitude",
            "dropoff_latitude",
            "dropoff_longitude",
        )
        df_gold = df_gold.groupBy("VendorID").agg(
            F.avg("trip_distance_cal").alias("avg_trip_duration")
        )
        df_gold.show()
        df_gold.writeTo("local.taxi_db.taxi_nyc_gld").using("iceberg").append()

  
    def _run_first_uc(self):
      """
      Description:
      Runs just ddl to create iceberg tables.

      """
      self._execute_ddl()

    def _run_second_uc(self):
      """
      Description:
      
      """
      self._execute_insert()

    def _run_third_uc(self):
      """
      Description: 

      copy-on-write concepts work by default which means....

      This behaviour can be changed to merge on read. In that case, new delete 
      file will be created.
      """
      self._execute_delete()
    
    def _run_wap_uc(self):
      """
      Description: write audit publish

      Explain theortically..
      Explain use case
      """
      #self._spark.sql("""
      #                ALTER TABLE local.taxi_db.taxi_nyc_br CREATE BRANCH v2_development
      # 
      #                """)
      DATA_TEST_FLG = True
      if DATA_TEST_FLG:
        self._spark.sql(
            """
            ALTER TABLE local.taxi_db.taxi_nyc_br SET TBLPROPERTIES ('write.wap.enabled'='true')
            """
            )
        self._spark.conf.set('spark.wap.branch', 'v2_development')
        self._execute_insert()
        version_df = self._spark.sql("""
            SELECT * from local.taxi_db.taxi_nyc_br VERSION AS OF 'v2_development'
            """)
        #version_df.show()
        version_df.count()
        """
        After this you can run DQ checks
        """
      
      self._spark.conf.unset('spark.wap.branch')
      main_df = self._spark.sql("""
            SELECT * from local.taxi_db.taxi_nyc_br
            """)
      main_df.show()

      self._spark.sql("CALL local.system.fast_forward('local.taxi_db.taxi_nyc_br','main','v2_development')")
      foward_main_df = self._spark.sql("""
            SELECT * from local.taxi_db.taxi_nyc_br
            """)
      foward_main_df.show()

    def _run_pipeline(self):
        """
        Description: Executes and create bronze silver gold transformation.
        """
        data_file = "/Users/intellify-sarojdongol/workspace/iceberg-meetup-101/datasets/yellow_tripdata_2015-01.csv"
        df_bronze = spark.read.csv(data_file, header=True)
        processed_brozen_df = self.bronze_builder(df_bronze)
        processed_silver_df = self.silver_builder(processed_brozen_df)
        processed_gold_df = self.gold_builder(processed_silver_df)
        processed_gold_df.show() 



if __name__ == "__main__":
    
    spark = (
        SparkSession.builder.appName("IcebergLocalDevelopment")
        .master("local[*]")
        .config("spark.executor.memory", "12g")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "spark-warehouse/iceberg")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .getOrCreate()
    )
    elt_processer = DataProcesserPipline(spark)
    fire.Fire(elt_processer)
