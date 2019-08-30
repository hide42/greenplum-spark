package org.apache.spark.sql.gpdf

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._

/**
  *This implicit class will allow easy access to streaming through external processes.
  * You need to import this class for using DataFrame Extension for GP connector.
  */
object GreenplumSparkExt {
  implicit def extraOperations(df: org.apache.spark.sql.DataFrame) = DataFrameExt(df)
}


case class DataFrameExt(df: org.apache.spark.sql.DataFrame) extends Serializable {
  /**
    * Create tmpTable for transaction copy.
    * @param tmpTable: tmp table-name
    * @param tableName: source table-name
    */
  private def createTMPGPTable(tmpTable: String, tableName: String)
                      (implicit factory: GPOptionsFactory) {
    createGPTableLike(tmpTable,tableName)(factory)
  }

  /**
    * CREATE TABLE AS creates a table and fills it with data computed by a SELECT command.
    * The table columns have the names and data types associated with the output columns of the SELECT,
    * however you can override the column names by giving an explicit list of new column names.
    * @param tableName: table-name
    * @param originalTableName: like as table,source table-name
    */
  def createGPTableLike(tableName: String, originalTableName: String)
                       (implicit factory: GPOptionsFactory) {
    val opts = factory.getJDBCOptions("")
    GPClient(opts).createTableLike(tableName,originalTableName)
  }

  /**
    * Create table.
    * @param tableName: table-name
    * @param schema: schema
    * @param distributedBy: distributedBy field (if fields, than join it into string comma-separated).
    */
  def createGPTable(tableName: String
                    , schema: String = "public"
                    , distributedBy: String = ""
                    , appendOnly: Boolean = false
                    , partitionBy: String = "")
                   (implicit factory: GPOptionsFactory) {
    val opts = factory.getJDBCOptions("")
    val strSchema = JdbcUtils.schemaString(df, opts.url, opts.createTableColumnTypes)
    GPClient(opts).createTable(tableName, schema, distributedBy, appendOnly, partitionBy, strSchema)
  }

  /**
    * Drop table.
    * @param tableName: table-name
    * @param schema: schema
    */
  def dropGPTable(tableName: String, schema: String = "public")
                 (implicit factory: GPOptionsFactory) {
    val opts = factory.getJDBCOptions("")
    GPClient(opts).dropTable(tableName, schema)
  }

  /**
    * Convert value from row into string
    * @param dataType: spark.sql types
    * @return function-get-field-string-format
    */
  private def makeConverter(dataType: DataType): (Row, Int) => String = dataType match {
    case StringType => (r: Row, i: Int) =>
      r.getString(i)
        .replaceAllLiterally("\\", "\\\\")
        .replaceAllLiterally(";", "\\;")
    case BooleanType => (r: Row, i: Int) => r.getBoolean(i).toString
    case ByteType => (r: Row, i: Int) => r.getByte(i).toString
    case ShortType => (r: Row, i: Int) => r.getShort(i).toString
    case IntegerType => (r: Row, i: Int) => r.getInt(i).toString
    case LongType => (r: Row, i: Int) => r.getLong(i).toString
    case FloatType => (r: Row, i: Int) => r.getFloat(i).toString
    case DoubleType => (r: Row, i: Int) => r.getDouble(i).toString
    case DecimalType() => (r: Row, i: Int) => r.getDecimal(i).toString
    case DateType =>
      (r: Row, i: Int) => r.getAs[Date](i).toString
    case TimestampType => (r: Row, i: Int) => r.getAs[Timestamp](i).toString
    case _ => (row: Row, ordinal: Int) => row.get(ordinal).toString
  }

  /**
    * Copy data to GP. With two attempts to load partition.
    * @param tableName: table name
    * @param schema: schema
    * @return resultOfCopy tuple (num-records df,num-records loaded into gp,time spent loading data)
    */
  def copyToGP(tableName: String
               , schema: String = "public")
              (implicit factory: GPOptionsFactory): (Long, Long, Double) = {
    val schemaDF: StructType = df.schema
    val opts = factory.getJDBCOptions(tableName)

    val valueConverters: Array[(Row, Int) => String] =
      schemaDF.map(s => makeConverter(s.dataType)).toArray
    df.rdd.mapPartitionsWithIndex({ (partId: Int, partition: Iterator[org.apache.spark.sql.Row]) => {
      if (partition.nonEmpty) {
        var res = GPClient(opts)
          .copyPartition(partition, schema + "." + tableName, valueConverters, partId, factory.timeout)
        //FOR numTries?
        if (res == (0, 0, 0)) {
          //Try rewrite :
          res = GPClient(opts)
            .copyPartition(partition, schema + "." + tableName, valueConverters, partId, factory.timeout, true)
        }
        Iterator.single(res)
      } else {
        Iterator.single((0L, 0L, 0.0d))
      }
    }
    }).fold((0L, 0L, 0.0d)) { case ((sended, writed, time), (s, w, t)) => (sended + s, writed + w, time + t) }
  }
  /**
    * Transaction copy data to GP. With two attempts to load partition.
    * @param tableName: table name
    * @param schema: schema
    * @return TransactionCopyGP contains resultOfCopy tuple, tmp-table, source table
    */
  def transactionCopyToGP(tableName: String
                          , schema: String = "public")
                         (implicit factory: GPOptionsFactory): TransactionCopyGP = {
    val tmpTable = s"tmp_transaction_$tableName"
    dropGPTable(tmpTable)
    createTMPGPTable(tmpTable, schema + "." + tableName)(factory)

    val result = copyToGP(tmpTable)
    TransactionCopyGP(result, schema + "." + tableName, tmpTable)
  }

}
