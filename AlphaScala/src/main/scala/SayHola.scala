import org.apache.spark.sql.SparkSession

object SayHola {
  def main(args: Array[String]): Unit = {
    print("Scala Spark")
    val spark = SparkSession.builder().appName(name = "MySpark").config("spark.master", "local").getOrCreate()
    println("Spark session is created")
    val sampleSeq = Seq((1, "Row1"), (2, "Row2"))
    val df = spark.createDataFrame(sampleSeq).toDF(colNames = "row_id", "row_name")
    df.show()
  }
}
