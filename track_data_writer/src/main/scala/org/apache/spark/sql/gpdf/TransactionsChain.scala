package org.apache.spark.sql.gpdf


case class TransactionsChain(firstTransaction: TransactionCopyGP) {
  private var sqlStmHeader = s"BEGIN;\n ${firstTransaction.sqlStmInsert};\n"

  def addTransaction(transaction: TransactionCopyGP): TransactionsChain = {
    sqlStmHeader = sqlStmHeader.concat(s"${firstTransaction.sqlStmInsert};\n")
    this
  }

  private val sqlStmFooter = "\nCOMMIT;"

  //todo return transaction
  def execute(implicit factory: GPOptionsFactory): Unit = {
    val opts = factory.getJDBCOptions("")
    GPClient(opts).executeQuery(s"$sqlStmHeader $sqlStmFooter")
  }
}
