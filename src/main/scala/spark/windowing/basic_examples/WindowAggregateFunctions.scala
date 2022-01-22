package spark.windowing.basic_examples

import models.Salary
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, max, min}
import utils.Utility

object WindowAggregateFunctions {

  def main(args: Array[String]): Unit = {

    val sparkSession = Utility.getSparkSession("WindowAggregateFunctions")
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

    val byDepName = Window.partitionBy("depName")

    val agg_sal = empSalary
      .withColumn("max_salary", max("salary").over(byDepName))
      .withColumn("min_salary", min("salary").over(byDepName))
      .withColumn("average", avg("salary").over(byDepName))

    agg_sal.show()

    //      +---------+-----+------+----------+----------+-----------------+
    //      |  depName|empNo|salary|max_salary|min_salary|          average|
    //      +---------+-----+------+----------+----------+-----------------+
    //      |  develop|    7|  4200|      6000|      4200|           5020.0|
    //      |  develop|    8|  6000|      6000|      4200|           5020.0|
    //      |  develop|    9|  4500|      6000|      4200|           5020.0|
    //      |  develop|   10|  5200|      6000|      4200|           5020.0|
    //      |  develop|   11|  5200|      6000|      4200|           5020.0|
    //      |personnel|    2|  3900|      3900|      3500|           3700.0|
    //      |personnel|    5|  3500|      3900|      3500|           3700.0|
    //      |    sales|    1|  5000|      5000|      4800|4866.666666666667|
    //      |    sales|    3|  4800|      5000|      4800|4866.666666666667|
    //      |    sales|    4|  4800|      5000|      4800|4866.666666666667|
    //      +---------+-----+------+----------+----------+-----------------+


    agg_sal
      .select("depname", "max_salary", "min_salary")
      .dropDuplicates()
      .show()

    //      +---------+----------+----------+
    //      |  depname|max_salary|min_salary|
    //      +---------+----------+----------+
    //      |  develop|      6000|      4200|
    //      |personnel|      3900|      3500|
    //      |    sales|      5000|      4800|
    //      +---------+----------+----------+
  }

}
