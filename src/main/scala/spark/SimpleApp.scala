package spark

import models.Salary
import  utils.Utility
import org.apache.spark.sql.SparkSession


object SimpleApp {

  def main(args: Array[String]) {


    val sparkSession = Utility.getSparkSession("SimpleApp")
    import sparkSession.implicits._

    val empSalary = Seq(
      Salary("sales", 1, 5000),
      Salary("personnel", 2, 3900),
      Salary("sales", 3, 4800),
      Salary("sales", 4, 4800),
      Salary("personnel", 5, 3500),
      Salary("develop", 7, 4200),
      Salary("develop", 8, 6000),
      Salary("develop", 9, 4500),
      Salary("develop", 10, 5200),
      Salary("develop", 11, 5200)).toDF()

    empSalary.show()

    sparkSession.stop()
  }
}
