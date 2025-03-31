package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
	val cd= queryRectangle.split(",")
	
    val bLX= cd(0).trim().toDouble
    val bLY= cd(1).trim().toDouble
    val tRX= cd(2).trim().toDouble
    val tRY= cd(3).trim().toDouble
	
    val qp= pointString.split(",")
    val qPX= qp(0).trim().toDouble
    val qPY= qp(1).trim().toDouble
	
    if (bLX > tRX || bLY > tRY) {
      throw new RuntimeException("Your points are not as you assumed; " +
        "bLX= "+bLX + ", bLY= "+bLY +
        ", tRX= "+tRX + ", tRY= "+ tRY)
    }
	
	qPX>= bLX && qPX<= tRX && qPY<= tRY && qPY>= bLY
    // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
