# SparkGIS
SparkGIS adds GIS functionalities to [SparkSQL](https://github.com/apache/spark/tree/master/sql) through:
* a user-defined type (*UDT*): **GeometryType**
* a class representing values of such type: **Geometry**
* a set of user-defined **Functions** (*UDF*) and predicates operating on one or two *Geometry* values

## Creating Geometry values
To create values, use factory methods in the *Geometry* object:
```
  import Geometry.WGS84
  val pt = Geometry.point(12.5, 14.6)
  val ln = Geometry.line((0,0), (20,50))
  val collection = Geometry.collection(pt, ln)
```
Each factory method has two argument lists:
* the first one is the set of 2D coordinates describing the geometry (one, many, many sequences, depending
on the geometry type)
* the second one is the coordinate reference system id; an implicit value *Geometry.WGS84* is provided for this

You can also create Geometry values from *WKB* (Well Known Binary), *WKT* (Well Known Text), and *GeoJSON* formats:
```
  val mp = Geometry.fromString("MULTIPOINT ((1 1), (2 2))")
  val ml = Geometry.fromGeoJSON("{\"type\":\"MultiLineString\",\"coordinates\":[[[12,13],[15,20]],[[7,9],[11,17]]]}}")
```

## Defining table schemas
Simply use the *GeometryType* instance as a type:
```
  val schema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("geo", GeometryType.Instance)
  ))	
```

## Creating RDDs
The *GeometryType* is able to produce *Geometry* values from any supported serialization format ("*WKB*, *WKT*, *GeoJSON*) as well as from schema-less JSON RDDs. So simply load your data and apply the schema as shown below:
```
  // using GeoJSON
  val data = Seq(
    "{\"id\":1,\"geo\":{\"type\":\"Point\",\"coordinates\":[1,1]}}",
    "{\"id\":2,\"geo\":{\"type\":\"LineString\",\"coordinates\":[[12,13],[15,20]]}}",
    "{\"id\":3,\"geo\":{\"type\":\"MultiLineString\",\"coordinates\":[[[12,13],[15,20]],[[7,9],[11,17]]]}}",
    ...
  )
  val rdd = sc.parallelize(data)
  val df = sqlContext.jsonRDD(rdd, schema)

  // or other means
  val data = Seq(
    Row(1, Geometry.point(1,1)),
    Row(2, Geometry.fromString("MULTIPOINT ((1 1), (2 2))"),
    ...
  )
  val rdd = sc.parallelize(data)
  val df = sqlContext.createDataFrame(rdd, schema)
```

## Using functions
Each function is defined as a method of the **Functions** object and can be used freely in any suitable context.
Moreover, they can be registered in the *SQLContext* and used inside *SparkSQL* queries:
```
  Functions.register(sqlContext)
  df.registerTempTable("features")
  result = sqlContext.sql("SELECT ST_Length(geo) FROM features")
```

## Using geometry methods
GIS functions are just aliasing methods from the class hierarchy rooting at *GisGeometry*.
*GisGeometry* hierarchy wraps *GeoTools* classes to provide a consistent interface - like returning
options instead of magic values or to model the absence of some property for some geometry type.

An instance of *GisGeometry* is wrapped by the SparkSQL *Geometry* type; the easiest way to access it
and invoke its methods is by importing *Geometry.ImplicitConversions*:
```
  import Geometry.ImplicitConversions._

  val l = Geometry.line((10.0,10.0), ...)
  if (!l.isEmpty) {
    val p: Option[Geometry] = l.startPoint
    ....
  }
```

Some method is also aliased as operator by *GeometryOperators* implicit class:
```
  import GeometryOperators._
  import Geometry.ImplicitConversions._

  val l1 = Geometry.line((10.0,10.0), (20.0,20.0), (10.0,30.0))
  val l2 = Geometry.line((20.0,20.0), (30.0,30.0), (40.0,40.0))
  if ((l1 <-> l2) < 50.0) { // distance less than 50
    ...
  }

```

## Build, test and doc
The project uses Maven as build system, so you should be comfortable with it.
If not, install Maven 3, cd in your SparkGIS directory and
```
  mvn package -DskipTests
  mvn test
  mvn scala:doc
```
You'll find the jar under the *target* directory, have run all available tests,
and generated the documentation under *target/site/scaladocs*.

## Credits
The *Geometry* value class is written on top of the [GeoTools](http://geotools.org/) library.

UDFs aim to adhere to [OGC Simple Feature Access](http://www.opengeospatial.org/standards/sfs) recommendation.
Name and documentation of GIS functions have been copied from [PostGIS](http://postgis.net/docs/manual-2.1/reference.html).

## Remarks
In order to work within jsonRDDs, Spark >= 1.4 is needed.

## Changelog
### 0.3.0
* Abandoned [ESRI Geometry](/Esri/geometry-api-java) library in favor of [GeoTools](http://geotools.org/)
* Moved to Scala 2.11
* Moved to Spark 1.6
* Added *ST_Perimeter*
* Added tolerance argument to *ST_Simplify*
* SRID in factory methods is now an implicit argument