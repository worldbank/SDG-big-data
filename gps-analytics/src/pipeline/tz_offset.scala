import org.locationtech.geomesa.spark.jts._

spark.withJTS

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val datapartnership = args(0)
val country = args(1)

val h3_idx = 6

val id_tz_lower = (7 - 2) * 3600 // Buffer TZ by 1
val id_tz_higher = (9 + 2) * 3600 // Buffer TZ by 1 

val mx_tz_lower = (-7 - 2) * 3600 // Buffer TZ by 1 hours
val mx_tz_higher = (-5 + 2) * 3600 // Buffer TZ by 1 hours


val tz_df_id = spark.sql(s"SELECT ZONE, TZ_OFFSET_SEC, h3index_6 as h3index FROM datapartnership.tz_h3_6 WHERE (TZ_OFFSET_SEC >= $id_tz_lower AND TZ_OFFSET_SEC <= $id_tz_higher)")

val tz_df_mx = spark.sql(s"SELECT ZONE, TZ_OFFSET_SEC, h3index_6 as h3index FROM datapartnership.tz_h3_6 WHERE (TZ_OFFSET_SEC >= $mx_tz_lower AND TZ_OFFSET_SEC <= $mx_tz_higher)")

// Veraset -- ID
var idDFH3 = spark.sql("SELECT * FROM datapartnership.veraset where country = 'ID'")
  .withColumn("h3index", geoToH3(col("lat"), col("lon"), lit(h3_idx)))
  .join(broadcast(tz_df_id), "h3index")
  .write.format("delta").mode("overwrite").partitionBy("date").save("/mnt/DataCollaboratives/local_gen/veraset_id_tz")

spark.sql("USE datapartnership")
spark.sql("CREATE TABLE IF NOT EXISTS veraset_id_tz USING DELTA LOCATION '/mnt/DataCollaboratives/local_gen/veraset_id_tz'")
spark.sql("OPTIMIZE veraset_id_tz")

// Veraset -- MX
var mxDFH3 = spark.sql("SELECT * FROM datapartnership.veraset where country = 'MX'")
  .withColumn("h3index", geoToH3(col("lat"), col("lon"), lit(h3_idx)))
  .join(broadcast(tz_df_mx),"h3index")
  .write.format("delta").mode("overwrite").partitionBy("date").save("/mnt/DataCollaboratives/local_gen/veraset_mx_tz")

spark.sql("USE datapartnership")
spark.sql("CREATE TABLE IF NOT EXISTS veraset_mx_tz USING DELTA LOCATION '/mnt/DataCollaboratives/local_gen/veraset_mx_tz'")
spark.sql("OPTIMIZE veraset_mx_tz")

spark.sql("USE datapartnership") spark.sql("CREATE TABLE IF NOT EXISTS unacast_m ...

spark.sql("USE datapartnership")
spark.sql("CREATE TABLE IF NOT EXISTS veraset_co_tz USING DELTA LOCATION '/mnt/DataCollaboratives/local_gen/veraset_co_tz'")
spark.sql("OPTIMIZE veraset_co_tz")

// IN - Veraset

var inDF = spark.sql("SELECT * from datapartnership.veraset where country = 'IN'")
  .withColumn("TZ_OFFSET_SEC", lit(19800))


inDF.write.format("delta").mode("overwrite").partitionBy("date").save("/mnt/DataCollaboratives/local_gen/veraset_in_tz")

spark.sql("USE datapartnership") spark.sql("CREATE TABLE IF NOT EXISTS veraset_i ...

// IN - Unacast

var inDF = spark.sql("SELECT * from datapartnership.unacast where country = 'IN'")
  .withColumn("TZ_OFFSET_SEC", lit(19800))


inDF.write.format("delta").mode("overwrite").partitionBy("date").save("/mnt/DataCollaboratives/local_gen/unacast_in_tz")

spark.sql("USE datapartnership")
spark.sql("CREATE TABLE IF NOT EXISTS unacast_in_tz USING DELTA LOCATION '/mnt/DataCollaboratives/local_gen/unacast_in_tz'")
spark.sql("OPTIMIZE unacast_in_tz")
