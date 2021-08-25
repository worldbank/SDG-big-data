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

//!!!! check that the timezone is correct for cuebiq data before running anything. See http://wrschneider.github.io/2019/09/01/timezones-parquet-redshift.html
require(spark.conf.get("spark.sql.session.timeZone") == "Etc/UTC")

// Here we need to get the environmental variables and the H3 functions

// Change this paths to read from yaml
val admin_fname = if (c("country") == "CO" || c("country") == "MX") "/admin_voronoi.csv" else "/admin.csv"

var admin = spark.read.option("header","true").option("multiLine", "true").option("mode", "FAILFAST").csv(c("admin_path") + "/" + c("country") + admin_fname)
admin = admin.where(col("geometry").isNotNull)
admin = admin.repartition(1000)
admin.createOrReplaceTempView("admin")

var query = "SELECT geom_id AS geom_id, ST_GeomFromText(geometry) as polygon_base FROM admin"
admin = spark.sql(query)
admin.createOrReplaceTempView("admin_with_polygons")

// query = "SELECT *, ST_MakeValid(polygon_base, false) as polygon FROM admin_with_polygons"
query = "SELECT *, ST_Buffer(polygon_base, 0) as polygon FROM admin_with_polygons"
admin = spark.sql(query)
admin.createOrReplaceTempView("admin_with_polygons")

query = "SELECT *, ST_Buffer(polygon, 0.005) as polygon_buff FROM admin_with_polygons"
admin = spark.sql(query)

val stops = spark.read.parquet(c("stop_locations_dir").toString())

val res = 10
val stopsH3 = stops.withColumn("h3index", geoToH3(col("lat"), col("lon"), lit(res)))
val adminH3 = admin.withColumn("h3index", multiPolygonToH3(col("polygon_buff"), lit(res))).select("geom_id", "h3index").withColumn("h3index", explode($"h3index"))

stopsH3.createOrReplaceTempView("stopsH3")
adminH3.createOrReplaceTempView("adminH3")

val query = """SELECT p.user_id
                    , p.lat
                    , p.lon
                    , p.median_accuracy
                    , p.cluster_label
                    , p.total_duration_stop_location
                    , p.t_start
                    , p.t_end
                    , p.duration
                    , p.total_pings_stop
                    , s.geom_id
                FROM stopsH3 as p
                INNER JOIN adminH3 AS s 
                ON (p.h3index = s.h3index)"""

var stops_geocoded = spark.sql(query)

stopsH3.cache()
adminH3.cache()

val out_table_temp = c("results_path_spark") + "stops_geocoded_temp/"
stops_geocoded.write.mode("overwrite").parquet(out_table_temp)

stopsH3.unpersist()
adminH3.unpersist()

val admin = spark.sql(s"select geom_id, polygon from admin_with_polygons")
val stops_geo = spark.read.parquet(out_table_temp)
stops_geo.cache()
admin.cache()
var stops_geo_pol = stops_geo.join(broadcast(admin), Seq("geom_id"))
stops_geo_pol.createOrReplaceTempView("stops_geo")

stops_geo_pol = spark.sql("SELECT *, ST_Point(cast(lon as Decimal(13,10)), cast(lat as Decimal(13,10))) as point FROM stops_geo")
stops_geo_pol.createOrReplaceTempView("stops_geo")

stops_geo_pol = spark.sql("SELECT * FROM stops_geo WHERE ST_Intersects(point, polygon)").drop("polygon", "point")

val out_table = c("results_path_spark") + "stops_geocoded/"
stops_geo_pol.write.mode("overwrite").parquet(out_table)

stops_geo.unpersist()
admin.unpersist()