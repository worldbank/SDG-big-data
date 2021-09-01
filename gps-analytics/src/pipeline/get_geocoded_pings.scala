import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}

import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialOperator.{JoinQuery}
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.formatMapper.GeoJsonReader
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

SedonaSQLRegistrator.registerAll(sqlContext)

val source = args(0)
val country = args(1)

//!!!! check that the timezone is correct for cuebiq data before running anything. See http://wrschneider.github.io/2019/09/01/timezones-parquet-redshift.html
require(spark.conf.get("spark.sql.session.timeZone") == "Etc/UTC")

// Here we need to get the environmental variables and the H3 functions
// Change this paths to read from yaml
var admin = spark.read.option("header","true").csv(c("admin_path") + "/" + c("country") + "/admin.csv")
admin.createOrReplaceTempView("admin")


var query = "SELECT geom_id AS geom_id, ST_GeomFromText(geometry) as polygon FROM admin"
admin = spark.sql(query)
admin.createOrReplaceTempView("admin_with_polygons")


query = "SELECT *, ST_Buffer(polygon, 0.005) as polygon_buff FROM admin_with_polygons"
admin = spark.sql(query)

// Read from env variables
val table = s"pings.${c("source")}_${c("country")}"

// "transfer" table to scala

var pings = spark.sql(s"SELECT lat, lon, accuracy, timestamp AS time, device_id AS user_id FROM ${c("input_table")} WHERE country = '${c("country")}'")
pings.filter($"lat" > -90 && $"lat" < 90 && $"lon" > -180 && $"lon" < 180 && $"accuracy" >= 0 && $"accuracy" <= 1000)
if (c("country") == "CO"){
  pings = pings.withColumn("offset", lit(-5*60*60)) //colombia offset
  pings.printSchema()
}

val res = 10
pings = pings.withColumn("time", col("time") + col("offset")).withColumn("h3index", geoToH3(col("lat"), col("lon"), lit(res)))

val adminH3 = admin.withColumn("h3index", multiPolygonToH3(col("polygon_buff"), lit(res))).select("geom_id", "h3index").withColumn("h3index", explode($"h3index"))

pings.createOrReplaceTempView("pingsH3")
adminH3.createOrReplaceTempView("adminH3")

val query = """SELECT p.time
                    , p.user_id
                    , p.lat
                    , p.lon
                    , p.accuracy
                    , s.geom_id
                FROM pingsH3 AS p
                INNER JOIN adminH3 AS s
                ON (p.h3index = s.h3index)"""

var pings_geocoded = spark.sql(query)

val out_table_temp = s"pings.${c("source")}_${c("country")}_geocoded_temp"
pings_geocoded.write.format("delta").mode("overwrite").saveAsTable(out_table_temp)

val out_table_temp = s"pings.${c("source")}_${c("country")}_geocoded_temp"

val admin = spark.sql(s"select geom_id, polygon from admin_with_polygon")
val pings_geo = spark.read.table(out_table_temp)
var pings_geo_pol = pings_geo.join(broadcast(admin), on='geom_id')
pings_geo_pol.createOrReplaceTempView("pings_geo")

val query = "SELECT *, ST_Point(cast(lon as Decimal(13,10)), cast(lat as Decimal(13,10))) as point FROM pings_geo"
pings_geo_pol = spark.sql(query)
pings_geo_pol.createOrReplaceTempView("pings_geo")

val query = "SELECT * FROM pings_geo WHERE ST_Intersects(point, polygon)"
pings_geo_pol = spark.sql(query).drop("polygon", "point", "valid")

val out_table_temp = s"pings.${c("source")}_${c("country")}_geocoded"
pings_geo_pol.write.format("delta").mode("overwrite").saveAsTable(out_table)