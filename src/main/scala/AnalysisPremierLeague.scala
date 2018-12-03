import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._

case class Test(A: String, B: String, C: String)

object AnalysisPremierLeague extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("Spark PL Analysis")
    .getOrCreate

  val schema = Encoders.product[Test].schema

  import spark.implicits._

  val ds = spark.read
    .option("header", "true")
    .option("charset", "UTF8")
    .schema(schema)
    .option("delimiter", ",")
    .csv("test.csv")
    .as[Test]

  val df1 = ds.createOrReplaceTempView("Test")

  val sqlDF1 = spark.sql("SELECT A,LENGTH(REPLACE(A, 'Man', '~')) - LENGTH(REPLACE(A, 'Man', '')) AS Occurrences FROM Test")

  sqlDF1.show

  val df = spark.read.option("header", "true").csv("stats.csv")

  df.createOrReplaceTempView("EPLStats20062007")

  val sqlDF = spark.sql("SELECT pen_goals_conceded, penalty_save" +
    " FROM EPLStats20062007")

  val df2 = sqlDF.withColumn("ratioPenaltySavedConceded", when(($"pen_goals_conceded" / $"penalty_save").isNull, "Impossible").otherwise($"pen_goals_conceded" / $"penalty_save"))

  //df2.show

  val sqlDF2 = spark.sql("SELECT CONCAT(team,'-',wins) FROM EPLStats20062007")

  //sqlDF2.show()

  val sqlDF3 = spark.sql("SELECT team, length(team) FROM EPLStats20062007 WHERE team LIKE '%Man%' " +
    "ORDER BY LENGTH(TEAM) DESC")

  //sqlDF3.show

  spark.stop

}
