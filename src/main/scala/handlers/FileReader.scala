package handlers

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object FileReader {

  private def sparkSession: SparkSession = {
    SparkSession
      .builder()
      .appName("Spark test")
      .master("local[*]")
      .getOrCreate()
  }

  this.sparkSession.sparkContext.setLogLevel("WARN")

  private def dropColumns(df: DataFrame, cols: List[String]): DataFrame = {
    df.select(df.columns.filter(col => !cols.contains(col)).map(colName => new Column(colName)): _*)
  }

  private def renameColumns(df: DataFrame): DataFrame = {
    df.withColumnRenamed("_c0", "hosts")
      .withColumnRenamed("_c3", "date")
      .withColumnRenamed("_c5", "request")
      .withColumnRenamed("_c6", "response")
      .withColumnRenamed("_c7", "bytes")
  }

  private def joinData(df: DataFrame, jldf: DataFrame): DataFrame = {
    df.union(jldf)
  }

  private def readFile(path: String, sep: String): DataFrame = {
    this.sparkSession.read.option("sep", sep).csv(path)
  }

  def saveFile(df: DataFrame, path: String): Unit = {
    df.write.csv(path)
  }

  def dropAndRenameColumns(df: DataFrame, cols: List[String]): DataFrame = {
    val removedCols = this.dropColumns(df, cols)
    this.renameColumns(removedCols)
  }

  def readAndJoinData(augPath: String,
                      julPath: String,
                      sep: String): DataFrame = {

    val aug = this.readFile(augPath, sep)
    val jul = this.readFile(julPath, sep)
    this.joinData(aug, jul)
  }
}
