/*
  Calculates salary difference between an employee
  and the employee with closest higher salary.
 */

package spark.windowing.use_case_based_examples

import models.Salary
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lead
import utils.Utility

object LeadSalaryDifference {

  def main(args: Array[String]): Unit = {

    val sparkSession = Utility.getSparkSession("LeadSalaryDifference")
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

    val windowSpec = Window.partitionBy("depName").orderBy("salary")
    val leadingSalary = lead("salary", 1).over(windowSpec)

    empSalary.withColumn("salary_diff", leadingSalary - $"salary").show()

//      +---------+-----+------+-----------+
//      |  depName|empNo|salary|salary_diff|
//      +---------+-----+------+-----------+
//      |  develop|    7|  4200|        300|
//      |  develop|    9|  4500|        700|
//      |  develop|   10|  5200|          0|
//      |  develop|   11|  5200|        800|
//      |  develop|    8|  6000|       null|
//      |personnel|    5|  3500|        400|
//      |personnel|    2|  3900|       null|
//      |    sales|    3|  4800|          0|
//      |    sales|    4|  4800|        200|
//      |    sales|    1|  5000|       null|
//      +---------+-----+------+-----------+
  }

}
