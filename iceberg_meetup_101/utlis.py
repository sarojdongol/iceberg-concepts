import pyspark.sql.functions as F
def cal_lat_log_dist(df, lat1, long1, lat2, long2):
        # Ref - https://en.wikipedia.org/wiki/Great-circle_distance#Formulae
        # We are using haversine formaula to derive this Distance between two Co-ordinates
        # Parameters:
        # Base DF with Four columns where it has LAT and LONG
        # Corresponding column name in Dataframe -> Cororidnate1 LAT1 LONG1
        # Corresponding column name in Dataframe -> Cororidnate2 LAT2 LONG2

        df = df.withColumn('trip_distance_cal' , \
            F.round((F.acos((F.sin(F.radians(F.col(lat1))) * F.sin(F.radians(F.col(lat2)))) + \
                   ((F.cos(F.radians(F.col(lat1))) * F.cos(F.radians(F.col(lat2)))) * \
                    (F.cos(F.radians(long1) - F.radians(long2))))
                       ) * F.lit(6371.0)), 4))
        return df