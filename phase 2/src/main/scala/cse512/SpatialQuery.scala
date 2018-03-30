package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{
	 val pointC = pointString.split(',');
	 val rectC = queryRectangle.split(',');
	 val minx = if(rectC(0).toDouble < rectC(2).toDouble) rectC(0).toDouble else rectC(2).toDouble;
	 val maxx = if(rectC(0).toDouble > rectC(2).toDouble) rectC(0).toDouble else rectC(2).toDouble;
	 val miny = if(rectC(1).toDouble < rectC(3).toDouble) rectC(1).toDouble else rectC(3).toDouble;
	 val maxy = if(rectC(1).toDouble > rectC(3).toDouble) rectC(1).toDouble else rectC(3).toDouble;
	 val result = pointC(0).toDouble>= minx && pointC(0).toDouble<= maxx && pointC(1).toDouble>= miny && pointC(1).toDouble<= maxy;
	if (result) true else false
	})
    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{
	 val pointC = pointString.split(',');
	 val rectC = queryRectangle.split(',');
	 val minx = if(rectC(0).toDouble < rectC(2).toDouble) rectC(0).toDouble else rectC(2).toDouble;
	 val maxx = if(rectC(0).toDouble > rectC(2).toDouble) rectC(0).toDouble else rectC(2).toDouble;
	 val miny = if(rectC(1).toDouble < rectC(3).toDouble) rectC(1).toDouble else rectC(3).toDouble;
	 val maxy = if(rectC(1).toDouble > rectC(3).toDouble) rectC(1).toDouble else rectC(3).toDouble;
	 val result = pointC(0).toDouble>= minx && pointC(0).toDouble<= maxx && pointC(1).toDouble>= miny && pointC(1).toDouble<= maxy;
	if (result) true else false
	})

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
	 val pt1 = pointString1.split(',');
	 val pt2 = pointString2.split(',');
	 val dist = Math.sqrt(Math.pow(pt2(0).toDouble - pt1(0).toDouble,2) + Math.pow(pt2(1).toDouble - pt1(1).toDouble,2));
	 if (dist <= distance) true else false
	})

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("pt1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("pt2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
	 val pt1 = pointString1.split(',');
	 val pt2 = pointString2.split(',');
	 val dist = Math.sqrt(Math.pow(pt2(0).toDouble - pt1(0).toDouble,2) + Math.pow(pt2(1).toDouble - pt1(1).toDouble,2));
	 if (dist <= distance) true else false
	})
	
    val resultDf = spark.sql("select * from pt1 p1, pt2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
