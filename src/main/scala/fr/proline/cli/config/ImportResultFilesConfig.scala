package fr.proline.cli.config

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import pureconfig._
import pureconfig.generic._
import pureconfig.generic.auto._
//import pureconfig.ConvertHelpers._

case class ImportResultFilesConfig(
  projectId: Option[Long] = None,
  instrumentConfigId: Option[Long] = None,
  peaklistSoftwareId: Option[Long] = None,
  decoyRegex: Option[String] = None,
  storeSpectrumMatches: Boolean = false,
  importerProperties: Option[Map[String, String]] = None
)

object ImportResultFilesConfig extends LazyLogging {
  
  private implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, SnakeCase))
  
  /*def parseConfig(configAsStr: String): ImportResultFilesConfig = {
    val rawConfig = ConfigFactory.load(configAsStr).getConfig("import-result-files-config")
    loadConfigOrThrow[ImportResultFilesConfig](rawConfig)
  }*/
  
  def loadAndParseConfig(configFile: String): ImportResultFilesConfig = {
    val rawConfig = ConfigFactory.parseFile(new java.io.File(configFile)).getConfig("import-result-files-config")
    //val parsedConfig = loadConfigOrThrow[ImportResultFilesConfig](rawConfig)
    val parsedConfig = ConfigSource.fromConfig(rawConfig).loadOrThrow[ImportResultFilesConfig] // .at(namespace)
    
    logger.debug("ImportResultFilesConfig config is: " + parsedConfig)
    
    parsedConfig
  }
}