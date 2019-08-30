package org.apache.spark.sql.gpdf

import java.io.{PipedInputStream, PipedOutputStream}
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.util.ThreadUtils
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import org.slf4j.LoggerFactory

import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Try
/**
  * Represents GPClient.
  * Logic with jdbc-connect.
  */
//private[spark] ?
case class GPClient(options: JDBCOptions) {

  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  private def getConn = JdbcUtils.createConnectionFactory(options)()

  /**
    * Execute sql-query
    * @param sqlQuery: query
    */
  def executeQuery(sqlQuery: String): Unit = {
    logger.warn(s"Execute sql:\n $sqlQuery")
    using(getConn) {
      conn => {
        using(conn.createStatement()) {
          statement => {
            statement.executeUpdate(sqlQuery)
          }
        }
      }
    }
  }
  /**
    * CREATE TABLE AS creates a table and fills it with data computed by a SELECT command.
    * The table columns have the names and data types associated with the output columns of the SELECT,
    * however you can override the column names by giving an explicit list of new column names.
    * @param table: table-name
    * @param originalTableName: like as table,source table-name
    */
  def createTableLike(table: String, originalTableName: String): Unit = {
    //IF NOT EXIST ONLY GP6+
    //INCLUDING ALL ONLY GP6+
    val sqlStmt =
      s"""s"CREATE TABLE $table
         | ( like $originalTableName including INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)
         | WITH (appendonly=true, orientation=column)"""".stripMargin
    executeQuery(sqlStmt)
  }

  /**
    * Drop table.
    * @param tableName: table-name
    * @param schema: schema
    */
  def dropTable(tableName:String, schema: String = "public"): Unit ={
    val sqlStmt =
      s"""DROP TABLE IF EXISTS $schema.$tableName"""
    executeQuery(sqlStmt)
  }

  /**
    * Create table.
    * @param tableName: table-name
    * @param schema: schema
    * @param distributedBy: distributedBy field (if fields, than join it into string comma-separated).
    */
  def createTable(tableName: String
                  , schema: String = "public"
                  , distributedBy: String = ""
                  , appendOnly: Boolean = false
                  , partitionBy: String = "",strSchema:String): Unit ={

    val sqlStmt =
      s"""
          CREATE TABLE $schema.$tableName ($strSchema)"""
    if (appendOnly)
      sqlStmt.concat(" WITH (appendonly=true, orientation=column)")
    if (!distributedBy.isEmpty)
      sqlStmt.concat(s" DISTRIBUTED BY ($distributedBy)")
    if (!partitionBy.isEmpty)
      sqlStmt.concat(s" PARTITION BY RANGE ($partitionBy) (DEFAULT PARTITION default)")
    try {
      executeQuery(sqlStmt)
    }catch {
      case Exception=>
      logger.warn(s"Table $tableName already exist")
    }
  }

  /**
    * Copy partition.
    * @param rows: partition of rdd
    * @param tableName: destination table
    * @param converter: get string fields from row
    * @param partId: partition id
    * @timeout: timeout promise
    * @again: for logging
    * @return resultOfCopy tuple (num-records df,num-records loaded into gp,time spent loading data)
    */
  def copyPartition(rows: Iterator[Row]
                    , tableName: String
                    , converter: Array[(Row, Int) => String]
                    , partId: Int
                    , timeout: Long
                    , again: Boolean = false): (Long, Long, Double) = {
    if (again)
      logger.error(s"Try copy partition num-$partId again")

    def convertRow(row: Row): Array[Byte] = {
      var i = 0
      val values = new Array[String](converter.length)
      while (i < converter.length) {
        if (!row.isNullAt(i)) {
          values(i) = converter(i).apply(row, i)
        } else {
          values(i) = "NULL"
        }
        i += 1
      }
      (values.mkString(";") + "\n").getBytes()
    }

    val promisedCopyNums = Promise[Long]
    val promisedWriteNums = Promise[Long]
    using(getConn) {
      conn => {
        val copyManager = new CopyManager(conn.asInstanceOf[BaseConnection])
        val output = new PipedOutputStream
        val input = new PipedInputStream(output, 8024)

        val sql = s"COPY $tableName" +
          s" FROM STDIN WITH NULL AS 'NULL' DELIMITER AS E';'"

        val writeThread = new Thread(s"pipe-thread-$partId") {
          override def run(): Unit = {

            try {
              logger.info(s"start write partition $partId into pipe")
              promisedWriteNums.complete(Try(
                {
                  var counter = 0
                  try {
                    rows.synchronized(
                      while(rows.hasNext)
                      {
                        counter += 1
                        output.write(convertRow(rows.next()))
                      }
                    )
                  } catch {
                    case e: Exception => {
                      logger.error(s"Error from write to copy thread $tableName in $partId partition")
                      e.printStackTrace()
                    }
                  }
                  counter
                }))
            } finally {
              output.close()
            }
          }
        }
        val copyThread = new Thread(s"copy-to-gp-thread-$partId") {
          override def run(): Unit = {
            try {
              promisedCopyNums.complete(Try(
                try {
                  copyManager.copyIn(sql, input)
                } catch {
                  case e: Exception => {
                    logger.error(s"Error from read-copy thread for $tableName in $partId partition")
                    e.printStackTrace()
                  }
                    0L
                }
              ))
            } finally {
              input.close()
            }
          }
        }
        logger.info(s"Start copy to gp partition $partId")
        val start = System.nanoTime() // monotonic time
        try {
          writeThread.start()
          copyThread.start()
          val numsSended = ThreadUtils
            .awaitResult(promisedWriteNums.future, Duration(timeout, TimeUnit.MILLISECONDS))
          val numsCopy = ThreadUtils
            .awaitResult(promisedCopyNums.future, Duration(timeout, TimeUnit.MILLISECONDS))
          val finish_time = (System.nanoTime() - start) / math.pow(10, 9)
          logger.warn(s"Copied $numsCopy records from $numsSended to Greenplum" +
            s" time taken: ${finish_time}s. Partition : $partId")
          (numsSended, numsCopy, finish_time)
        }
        catch {
          case e: Exception => {
            logger.error(s"Timeout gp for $tableName")
            logger.error(e.getMessage)
            e.printStackTrace()
            (0, 0, 0)
          }
        }
        finally {
          writeThread.interrupt()
          copyThread.interrupt()
          writeThread.join()
          copyThread.join()
        }
      }
    }
  }

  private def using[A, B <: {def close() : Unit}](closeable: B)(f: B => A): A =
    try {
      f(closeable)
    }
    finally {
      closeable.close()
    }

}
