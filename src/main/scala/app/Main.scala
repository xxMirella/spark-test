package app

import conf.FilePaths
import engine.Engine
import engine.enums.Columns
import handlers.FileReader
import org.apache.spark.sql.DataFrame


object Main extends App {

  val data: DataFrame = FileReader.readAndJoinData(FilePaths.augLog, FilePaths.julLog, " ")
  val treatedDf = FileReader.dropAndRenameColumns(data, Columns.COLUMNS_TO_REMOVE)

  Engine.uniqueHosts(treatedDf)
  Engine.total404Errors(treatedDf)
  Engine.rankUrlsWith404Errors(treatedDf)
  Engine.errors404ByDay(treatedDf)
  Engine.totalBytes(treatedDf)
}
