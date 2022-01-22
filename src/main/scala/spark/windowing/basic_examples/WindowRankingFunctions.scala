package spark.windowing.basic_examples

import models.Salary
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{dense_rank, ntile, percent_rank, rank}
import utils.Utility

object WindowRankingFunctions {

  def main(args: Array[String]): Unit = {

    val sparkSession = Utility.getSparkSession("WindowRankingFunctions")
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

    val winSpec = Window.partitionBy("depName").orderBy($"salary".desc)

    val rank_df = empSalary.withColumn("rank", rank().over(winSpec))
    rank_df.show()

    //      +---------+-----+------+----+
    //      |  depName|empNo|salary|rank|
    //      +---------+-----+------+----+
    //      |  develop|    8|  6000|   1|
    //      |  develop|   10|  5200|   2|
    //      |  develop|   11|  5200|   2|
    //      |  develop|    9|  4500|   4|
    //      |  develop|    7|  4200|   5|
    //      |personnel|    2|  3900|   1|
    //      |personnel|    5|  3500|   2|
    //      |    sales|    1|  5000|   1|
    //      |    sales|    3|  4800|   2|
    //      |    sales|    4|  4800|   2|
    //      +---------+-----+------+----+

    val dense_rank_df = empSalary.withColumn("dense_rank", dense_rank().over(winSpec))
    dense_rank_df.show()

    //      +---------+-----+------+----------+
    //      |  depName|empNo|salary|dense_rank|
    //      +---------+-----+------+----------+
    //      |  develop|    8|  6000|         1|
    //      |  develop|   10|  5200|         2|
    //      |  develop|   11|  5200|         2|
    //      |  develop|    9|  4500|         3|
    //      |  develop|    7|  4200|         4|
    //      |personnel|    2|  3900|         1|
    //      |personnel|    5|  3500|         2|
    //      |    sales|    1|  5000|         1|
    //      |    sales|    3|  4800|         2|
    //      |    sales|    4|  4800|         2|
    //      +---------+-----+------+----------+


    // This function will return the relative (percentile) rank within the partition.
    val percent_rank_df = empSalary.withColumn("percent_rank", percent_rank().over(winSpec))
    percent_rank_df.show()

    //      +---------+-----+------+------------+
    //      |  depName|empNo|salary|percent_rank|
    //      +---------+-----+------+------------+
    //      |  develop|    8|  6000|         0.0|
    //      |  develop|   10|  5200|        0.25|
    //      |  develop|   11|  5200|        0.25|
    //      |  develop|    9|  4500|        0.75|
    //      |  develop|    7|  4200|         1.0|
    //      |personnel|    2|  3900|         0.0|
    //      |personnel|    5|  3500|         1.0|
    //      |    sales|    1|  5000|         0.0|
    //      |    sales|    3|  4800|         0.5|
    //      |    sales|    4|  4800|         0.5|
    //      +---------+-----+------+------------+

    /*
    This function can further sub-divide the window into n groups based on a window specification or partition
    For example, if we need to divide the departments further into say three groups we can specify ntile as 3.
     */
    val ntile_df = empSalary.withColumn("ntile", ntile(3).over(winSpec))
    ntile_df.show()

    //      +---------+-----+------+-----+
    //      |  depName|empNo|salary|ntile|
    //      +---------+-----+------+-----+
    //      |  develop|    8|  6000|    1|
    //      |  develop|   10|  5200|    1|
    //      |  develop|   11|  5200|    2|
    //      |  develop|    9|  4500|    2|
    //      |  develop|    7|  4200|    3|
    //      |    sales|    1|  5000|    1|
    //      |    sales|    3|  4800|    2|
    //      |    sales|    4|  4800|    3|
    //      |personnel|    2|  3900|    1|
    //      |personnel|    5|  3500|    2|
    //      +---------+-----+------+-----+


  }
}
