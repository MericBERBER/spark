package spark.windowing.basic_examples

import models.Salary
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.max
import utils.Utility

object CustomWindowDefinitions {

  def main(args: Array[String]): Unit = {

    val sparkSession = Utility.getSparkSession("CustomWindowDefinitions")
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

    val winSpec = Window
      .partitionBy("depName")
      .orderBy("salary")
      .rangeBetween(100L, 300L)

    val range_between_df = empSalary.withColumn("max_salary", max("salary").over(winSpec))
    range_between_df.show()

    //    +---------+-----+------+----------+
    //    |  depName|empNo|salary|max_salary|
    //    +---------+-----+------+----------+
    //    |  develop|    7|  4200|      4500| Partition bounds for this row: [4300, 4600]
    //    |  develop|    9|  4500|      null| Partition bounds for this row: [4600, 4800] -> No data exists in that range.
    //    |  develop|   10|  5200|      null|
    //    |  develop|   11|  5200|      null|
    //    |  develop|    8|  6000|      null|
    //    |personnel|    5|  3500|      null|
    //    |personnel|    2|  3900|      null|
    //    |    sales|    3|  4800|      5000|
    //    |    sales|    4|  4800|      5000|
    //    |    sales|    1|  5000|      null|
    //    +---------+-----+------+----------+

    val unboundedFollowingWinSpec = Window
      .partitionBy("depName")
      .orderBy("salary")
      .rangeBetween(300L, Window.unboundedFollowing)

    val range_unbounded_df = empSalary.withColumn("max_salary", max("salary").over(unboundedFollowingWinSpec))
    range_unbounded_df.show()

    //      +---------+-----+------+----------+
    //      |  depName|empNo|salary|max_salary|
    //      +---------+-----+------+----------+
    //      |  develop|    7|  4200|      6000| Partition bounds for this row: [4500, limitless]
    //      |  develop|    9|  4500|      6000|
    //      |  develop|   10|  5200|      6000|
    //      |  develop|   11|  5200|      6000|
    //      |  develop|    8|  6000|      null| Partition bounds for this row: [6300, limitless]
    //      |personnel|    5|  3500|      3900| Partition bounds for this row: [3800, limitless]
    //      |personnel|    2|  3900|      null|
    //      |    sales|    3|  4800|      null|
    //      |    sales|    4|  4800|      null|
    //      |    sales|    1|  5000|      null|
    //      +---------+-----+------+----------+


    val rowsBetweenWinSpec = Window
      .partitionBy("depName")
      .orderBy("salary")
      .rowsBetween(-1, 1)

    val rows_between_df = empSalary.withColumn("max_salary", max("salary").over(rowsBetweenWinSpec))
    rows_between_df.show()

    //      +---------+-----+------+----------+
    //      |  depName|empNo|salary|max_salary|
    //      +---------+-----+------+----------+
    //      |  develop|    7|  4200|      4500|
    //      |  develop|    9|  4500|      5200|
    //      |  develop|   10|  5200|      5200|
    //      |  develop|   11|  5200|      6000|
    //      |  develop|    8|  6000|      6000|
    //      |personnel|    5|  3500|      3900|
    //      |personnel|    2|  3900|      3900|
    //      |    sales|    3|  4800|      4800|
    //      |    sales|    4|  4800|      5000|
    //      |    sales|    1|  5000|      5000|
    //      +---------+-----+------+----------+
  }


}
