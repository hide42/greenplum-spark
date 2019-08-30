package org.apache.spark.sql.gpdf

case class TransactionCopyGP(resultOfCopy:(Long, Long, Double), tableName:String, tmpTableName:String) {

  val sqlStmInsert = s"INSERT INTO $tableName SELECT * FROM $tmpTableName"

  /**
    * Insert data from temp table to source table
    * @return resultOfCopy tuple
    */
  def commit()(implicit factory: GPOptionsFactory):(Long, Long, Double) ={
    val opts = factory.getJDBCOptions("")
    GPClient(opts).executeQuery(sqlStmInsert)
    GPClient(opts).dropTable(tmpTableName)
    resultOfCopy
  }
  /**
    * If u need few transaction.
    * @return TransactionsChain
    */
  def buildTransactionChain:TransactionsChain={
    TransactionsChain(this)
  }
}
