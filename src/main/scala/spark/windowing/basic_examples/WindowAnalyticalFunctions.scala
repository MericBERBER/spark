package spark.windowing.basic_examples

import models.Salary
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lag, lead}
import utils.Utility

object WindowAnalyticalFunctions {

  def main(args: Array[String]): Unit = {

    val sparkSession = Utility.getSparkSession("WindowAnalyticalFunctions")
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


    val winSpec = Window.partitionBy("depName").orderBy("salary")

    /*
      The lag function takes 3 arguments (lag(col, count = 1, default = None))
      col: defines the columns on which function needs to be applied.
      count: for how many rows we need to look back.
      default: defines the default value.
     */
    val lag_df = empSalary.withColumn("lag", lag("salary", 2).over(winSpec))
    lag_df.show()

    //      +---------+-----+------+----+
    //      |  depName|empNo|salary| lag|
    //      +---------+-----+------+----+
    //      |  develop|    7|  4200|null|
    //      |  develop|    9|  4500|null|
    //      |  develop|   10|  5200|4200|
    //      |  develop|   11|  5200|4500|
    //      |  develop|    8|  6000|5200|
    //      |    sales|    4|  4800|null|
    //      |    sales|    3|  4800|null|
    //      |    sales|    1|  5000|4800|
    //      |personnel|    5|  3500|null|
    //      |personnel|    2|  3900|null|
    //      +---------+-----+------+----+

    /*
      lead function takes 3 arguments (lead(col, count = 1, default = None))
      col: defines the columns on which the function needs to be applied.
      count: for how many rows we need to look forward/after the current row.
      default: defines the default value.
     */
    val lead_df = empSalary.withColumn("lead", lead("salary", 2).over(winSpec))
    lead_df.show()

    //      +---------+-----+------+----+
    //      |  depName|empNo|salary|lead|
    //      +---------+-----+------+----+
    //      |  develop|    7|  4200|5200|
    //      |  develop|    9|  4500|5200|
    //      |  develop|   10|  5200|6000|
    //      |  develop|   11|  5200|null|
    //      |  develop|    8|  6000|null|
    //      |personnel|    5|  3500|null|
    //      |personnel|    2|  3900|null|
    //      |    sales|    3|  4800|5000|
    //      |    sales|    4|  4800|null|
    //      |    sales|    1|  5000|null|
    //      +---------+-----+------+----+
  }

}
