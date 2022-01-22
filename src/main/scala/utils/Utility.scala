package utils

import com.sun.research.ws.wadl.Application
import org.apache.spark.sql.SparkSession

object Utility {

  def getSparkSession(applicationName: String): SparkSession = {

    SparkSession.builder.
      master("local").
      appName(applicationName).
      getOrCreate()


  }


}
