package org.apache.spark.sql.gpdf

import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.util.Utils

/**
  * Options for connector
  * @param params: map with parameters (url,copyTimeout,jdbc params)
  */
case class GPOptionsFactory(params: Map[String, String]) {
  require(params.contains("url"))
  val timeout:Long = Utils.timeStringAsMs(params.getOrElse("copyTimeout", "1h"))

  /**
    * Create JDBC Options for tableName
    * @param tableName
    * @return JDBCOptions
    */
  def getJDBCOptions(tableName: String): JDBCOptions = {
    new JDBCOptions(CaseInsensitiveMap(params
      + ("driver" -> "org.postgresql.Driver",
      "dbtable" -> tableName)))
  }
}
