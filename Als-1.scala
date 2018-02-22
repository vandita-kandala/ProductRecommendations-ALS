

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.Rating;

object Als {

  def main(args: Array[String]) {

    var x = 0;
    var user = false;
    var product = false;
    var id = 0;

    if (args.length == 0) {
      println("Please provide some input");
    } else {
      for (x <- 0 to args.length) {
        if (!user && !product) {

          if (args(x) == "-u") {
            user = true;
            id = args(x + 1).toInt;
          } else if (args(x) == "-p") {
            product = true;
            id = args(x + 1).toInt;
          }
        }
      }

      // create Spark context with Spark configuration
      val sc = new SparkContext(new SparkConf().setAppName("ALS Recommender").setMaster("local"))

      val dataset = sc.textFile("ALSInput/amazon-reviews-ratings.txt")

      val ratings = dataset.map {
        record =>
          val recordArray = record.split("::");
          Rating(recordArray(0).toInt, recordArray(1).toInt, recordArray(2).toDouble)
      }

      ratings.first();

      val trainModel = ALS.train(ratings, 50, 10, 0.01);
      //testing :

      if (user) {

        val topTenProducts = trainModel.recommendProducts(id, 10).map { x => x.product }
        topTenProducts.foreach { println };
      }

      if (product) {

        val topTenUsers = trainModel.recommendUsers(id, 10).map { x => x.user }
        topTenUsers.foreach { println };
      }

    }

  }
}