package fr.proline.cli.config

import scala.collection.JavaConversions._
import scala.io.Source

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import pureconfig._
import pureconfig.generic._
import pureconfig.generic.auto._

import fr.profi.util.serialization.ProfiJson
import fr.proline.core.om.model.msq.ExperimentalDesign


case class QuantifyDataSetConfig(
  @JsonDeserialize(contentAs = classOf[java.lang.Long] )
  projectId: Option[Long] = None,
  datasetName: String = "Unnamed data set",
  datasetDescription: String = "",
  quantMethodName: String = "label_free"
) {
  var experimentalDesign: Option[ExperimentalDesign]  = None
  var quantitationConfig: Option[java.util.Map[String,Object]]  = None
}

object QuantifyDataSetConfig extends LazyLogging {
  
  private implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, SnakeCase))
  
  def config2jsonStr(config: Config): String = {
    config.root().render(
     com.typesafe.config.ConfigRenderOptions.concise().setJson(true)
    )
  }
  
  /*def loadAndParseConfig(configFile: String): QuantifyDataSetConfig = {
    val jsonStr = Source.fromFile(configFile).getLines.mkString("\n")
    val parsedConfig = ProfiJson.deserialize[QuantifyDataSetConfig](jsonStr)
    
    logger.debug("Parsed config is: " + parsedConfig)
    
    parsedConfig
  }*/
  
  def loadAndParseConfig(configFile: String): QuantifyDataSetConfig = {
    val rawConfig = ConfigFactory.parseFile(new java.io.File(configFile)).getConfig("quantify-dataset-config")
    val jsonStr = config2jsonStr(rawConfig)
    val parsedConfig = ProfiJson.deserialize[QuantifyDataSetConfig](jsonStr)
    
    logger.debug("QuantifyDataSetConfig config is: " + parsedConfig)
    
    parsedConfig
  }
  
  /*def loadAndParseConfig(configFile: String): QuantifyDataSetConfig = {
    val rawConfig = ConfigFactory.parseFile(new java.io.File(configFile)).getConfig("quantify-dataset-config")
    
    val parsedConfig = loadConfigOrThrow[QuantifyDataSetConfig](rawConfig)
    
    parsedConfig.experimentalDesign = Some( parseExperimentalDesign(rawConfig.getConfig("experimental_design")) )
    parsedConfig.quantitationConfig = Some( parseQuantitationConfig(rawConfig.getConfig("quantitation_config")) )
    
    logger.debug("Parsed config is: " + parsedConfig)
    
    parsedConfig
  }*/

  /*private def parseExperimentalDesign(rawConfig: Config): ExperimentalDesign = {
    val paramsMap = rawConfig.root().unwrapped()
    ProfiJson.getObjectMapper().convertValue(paramsMap, classOf[ExperimentalDesign])
  }

  private def parseQuantitationConfig(rawConfig: Config): java.util.Map[String,Object] = {
    val paramsMap = rawConfig.root().unwrapped()
    ProfiJson.getObjectMapper().convertValue(paramsMap, classOf[java.util.Map[String,Object]])
  }*/

}