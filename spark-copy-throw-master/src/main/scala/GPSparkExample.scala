import com.typesafe.scalalogging.Logger
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.sql.gpdf.GPOptionsFactory
import org.slf4j.LoggerFactory
import org.apache.spark.sql.gpdf.GreenplumSparkExt._

/**
  * Represent loading data into gp from kafka.
  */
object GPSparkExample {
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  /**
    * Read and load records from the specified broker and topic.
    * @param broker
    * @param topic
    */
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .appName("test_gp")
      .master("local[2]")
      .getOrCreate()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> args(0),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka_group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "heartbeat.interval.ms" -> "30000",
      "session.timeout.ms" -> "90000",
      "fetch.max.wait.ms" -> "10000",
      "request.timeout.ms" -> "100000"
    )

    val topic = args(1)

    val sc = ss.sparkContext
    val ssc = new StreamingContext(sc, Seconds(60))
    sc.setLogLevel("WARN")

    val rawDataFeed = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topic.split(","), kafkaParams)
    )

    val main_table = "spark_table"

    rawDataFeed.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (!rdd.isEmpty()) {
        try{
          println(s"Offset count: ${rdd.count()}")
          val df = ss.read.json(rdd.map(record=>record.value()))

          implicit val gpFactory = GPOptionsFactory(Map("url" -> ss.conf.get("spark.psql.jdbc.url"),
            "dbschema" -> ss.conf.get("spark.psql.dbschema"),
            "user" -> ss.conf.get("spark.psql.user"),
            "password" -> ss.conf.get("spark.psql.password")))
          df.createGPTable(main_table)
          val loading = df.transactionCopyToGP(main_table)
          //w8 loading data to tmp table & try tmp->main_table
          loading.commit()
          //if ok => all batches loaded => can commit offsets
          rawDataFeed.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        } catch {
          case e: MatchError => {
            logger.error("ERROR: " + e.getMessage)
          }
        }

      }
    })
    ssc.start()

    try {
      ssc.awaitTermination()
      println("*** streaming terminated ***")
      ssc.stop()
      sc.stop()
    } catch {
      case e: Exception => {
        logger.error("Terminated")
      }
    }
  }
}