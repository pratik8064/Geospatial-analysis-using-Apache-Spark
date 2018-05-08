package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

	val pointC = pointString.split(',');
	 val rectC = queryRectangle.split(',');
	 val minx = if(rectC(0).toDouble < rectC(2).toDouble) rectC(0).toDouble else rectC(2).toDouble
	 val maxx = if(rectC(0).toDouble > rectC(2).toDouble) rectC(0).toDouble else rectC(2).toDouble
	 val miny = if(rectC(1).toDouble < rectC(3).toDouble) rectC(1).toDouble else rectC(3).toDouble
	 val maxy = if(rectC(1).toDouble > rectC(3).toDouble) rectC(1).toDouble else rectC(3).toDouble
	 val result = pointC(0).toDouble>= minx && pointC(0).toDouble<= maxx && pointC(1).toDouble>= miny && pointC(1).toDouble<= maxy
    return result // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
