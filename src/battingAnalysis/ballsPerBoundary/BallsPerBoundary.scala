package sparkSQLBattingAnalysis


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{bround, col};

object BallsPerBoundary {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BallsPerBoundary");
    var sc = new SparkContext(conf);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc);

    import sqlContext.implicits._;
    var IPLRdd = sc.textFile("/user/input/IPLMatchesBallByBall.csv", 5).map(_.split(","));

    case class IPLBallByBall(ball: String, innings: Int, delivery: Float, batting_team: String, striker: String, non_striker: String, bowler: String, runs_in_that_deleivery: Int, extras: Int, dissmissal_type: String, dissmissed_player: String, team1: String, team2: String, dom: String, season: Int);
    var IPLDf = IPLRdd.map(r => IPLBallByBall(r(0), r(1).toInt, r(2).toFloat, r(3), r(4), r(5), r(6), r(7).toInt, r(8).toInt, r(9), r(10), r(11), r(12), r(13), r(14).toInt)).toDF;
    IPLDf.createOrReplaceTempView("IPLDataTable");
    val boundariesPerBall = sqlContext.sql("select striker , SUM(CASE WHEN runs_in_that_deleivery==4 or runs_in_that_deleivery==6 THEN 1 else 0 END ) as boundaries , SUM(CASE WHEN extras == 0 OR runs_in_that_deleivery!=0 THEN 1 ELSE 0 END) as balls from IPLDataTable where innings <3 AND season>=2015  Group by striker having SUM(runs_in_that_deleivery)>300 ");
    var resultDF = boundariesPerBall.select(col("striker"), bround(col("balls").divide(col("boundaries")), 2) as "balls_per_boundary").orderBy(col("balls_per_boundary").asc);
    resultDF.coalesce(1).write.mode("overwrite").csv("<Path for output file>");

  }
}
