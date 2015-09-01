# SparkGIS
SparkGIS adds GIS functionalities to [SparkSQL](https://github.com/apache/spark/tree/master/sql) through:
* a user-defined type (*UDT*): **GeometryType**
* a class representing values of such type: **Geometry**
* a set of user-defined **Functions** (*UDF*) and predicates operating on one or two *Geometry* values

## Creating Geometry values
To create values, use factory methods in the *Geometry* object:
```
	val pt = Geometry.point(12.5, 14.6)
	val ln = Geometry.line((0,0), (20,50))
	val collection = Geometry.aggregate(pt, ln)
```

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

## Credits
The *Geometry* value class is written on top of the [ESRI Geometry](/Esri/geometry-api-java) library.

UDFs aim to adhere to [OGC Simple Feature Access](http://www.opengeospatial.org/standards/sfs) recommendation.
When some of them were unavailable from the ESRI library, those have been implemented mimicking [PostGIS](http://postgis.net/docs/manual-2.1/reference.html) behaviour.

## Remarks
In order to work within jsonRDDs, Spark 1.4 is needed.
