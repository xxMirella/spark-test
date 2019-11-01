package engine

import org.apache.spark.sql._
import org.apache.spark.sql.types.LongType

object Engine {

  private def filter404Errors(df: DataFrame): Dataset[Row] = {
    df.filter(df("response").cast(LongType) === 404)
  }

  def uniqueHosts(df: DataFrame): Unit = {
    val total = df.select("hosts").distinct().count()
    println("Número total de hosts únicos: " + total)
  }

  def total404Errors(df: DataFrame): Unit = {
    val total = this.filter404Errors(df).count()
    println("Total de error 404" + total)
  }

  def rankUrlsWith404Errors(df: DataFrame): Unit = {
    val errors = this.filter404Errors(df)
    val total = errors.groupBy("hosts").count.sort(functions.col("count").desc).limit(5)
    println("Rank de hosts com mais erros 404: ")
    total.show(false)
  }

  def errors404ByDay(df: DataFrame): Unit = {
    val errors = this.filter404Errors(df)
    val createNewCol = errors.withColumn("day", df("date").substr(2, 11))
    val filteredByDay = createNewCol.groupBy("day").count.sort(functions.col("day").asc)

    println("Total de erros 404 por dia: ")
    filteredByDay.show(numRows = 100, truncate = false)
  }

  def totalBytes(df: DataFrame): Unit = {
    println("Total de bytes retornados: ")
    df.agg(functions.sum("bytes").alias("sum")).show(false)
  }
}
