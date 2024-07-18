package fr.proline.cli.config

import scala.collection.JavaConversions._

import com.thetransactioncompany.jsonrpc2.util.NamedParamsRetriever

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.config.impl.SimpleConfigObject
import com.typesafe.config.ConfigObject

import pureconfig._
import pureconfig.generic._
import pureconfig.generic.auto._

import fr.proline.core.algo.msi.validation.ITargetDecoyAnalyzer
import fr.proline.core.algo.msi.validation.BasicTDAnalyzer
import fr.proline.cortex.api.service.dps.msi.FilterConfig
import fr.proline.cortex.api.service.dps.msi.ProtSetValidatorConfig
import fr.proline.core.algo.msi.validation.BuildOptimizablePeptideMatchFilter
import fr.proline.cortex.api.service.dps.msi.ValidateResultSetService
import fr.proline.core.algo.msi.validation.BuildProteinSetValidator
import fr.proline.core.algo.msi.validation.ProtSetValidationMethods
import fr.proline.core.algo.msi.validation.IPeptideMatchValidator
import fr.proline.core.algo.msi.filtering.IProteinSetFilter
import fr.proline.core.algo.msi.validation.BuildPeptideMatchValidator
import fr.proline.core.algo.msi.validation.TargetDecoyModes
import fr.proline.core.algo.msi.filtering.IPeptideMatchFilter
import fr.proline.cortex.api.service.dps.msi.PepMatchValidatorConfig
import fr.proline.core.algo.msi.validation.BuildPeptideMatchFilter
import fr.proline.core.algo.msi.validation.IProteinSetValidator
import fr.proline.core.service.msi.ValidationConfig
import fr.proline.core.algo.msi.scoring.PepSetScoring
import fr.profi.util.serialization.ProfiJson
import fr.proline.core.algo.msi.validation.BuildProteinSetFilter

case class ValidateDataSetConfig(
  projectId: Option[Long] = None,
  datasetName: String = "Unnamed data set",
  mergeResultSets: Boolean = false
) {
  var validationConfig: Option[ValidationConfig]  = None
}

object ValidateDataSetConfig extends LazyLogging {
  
  private implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, SnakeCase))
  
  def config2jsonStr(config: Config): String = {
    config.root().render(
     com.typesafe.config.ConfigRenderOptions.concise().setJson(true)
    )
  }
  /*implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getOptBoolean(path: String): Option[Boolean] = if (underlying.hasPath(path)) {
       Some(underlying.getBoolean(path))
    } else {
       None
    }
    def getOptString(path: String): Option[String] = if (underlying.hasPath(path)) {
       Some(underlying.getString(path))
    } else {
       None
    }
  }*/
  
  def loadAndParseConfig(configFile: String): ValidateDataSetConfig = {
    val rawConfig = ConfigFactory.parseFile(new java.io.File(configFile)).getConfig("validate-dataset-config")
    
    /*val testConfig = ConfigFactory.parseString("""
  project_id = 2
  dataset_name = "my dataset"
  merge_result_sets = true
""")*/
    
    val parsedConfig = ConfigSource.fromConfig(rawConfig).loadOrThrow[ValidateDataSetConfig]
    
    parsedConfig.validationConfig = Some( parseValidationConfig(rawConfig.getConfig("validation_config")) )
    logger.debug("ValidateDataSetConfig config is: " + parsedConfig)
    
    parsedConfig
  }
  
  import ValidateResultSetService.PROCESS_METHOD._
  
  private def parseValidationConfig(rawConfig: Config): ValidationConfig = {

    // val pepMatchFilters =  .parsePepMatchFilters(params,targetRS )
    val pepMatchFilters = parsePepMatchFilters(rawConfig)

    // Create a default TD analyzer 
    val tdAnalyzer: Option[ITargetDecoyAnalyzer] = Some(new BasicTDAnalyzer(TargetDecoyModes.CONCATENATED))
    val pepMatchValidator = parsePepMatchValidator(rawConfig, tdAnalyzer)
    val pepSetScoring = Option(rawConfig.getString(PEP_SET_SCORE_TYPE_PARAM)).map(PepSetScoring.withName(_))
    val protSetFilters = parseProtSetFilters(rawConfig)
    val protSetValidator = parseProtSetValidator(rawConfig)

    ValidationConfig(
      tdAnalyzer = None,
      pepMatchPreFilters = pepMatchFilters,
      pepMatchValidator = pepMatchValidator,
      pepSetScoring = pepSetScoring,
      protSetFilters = protSetFilters,
      protSetValidator = protSetValidator
    )
  }

  private def parseFilterConfig(rawConfig: Config): FilterConfig = {
    val configAsMap = rawConfig.root().unwrapped()
    
    val paramName = rawConfig.getString(ValidateResultSetService.PARAMETER_PARAM_NAME)
    val thresholdValue = configAsMap.get(ValidateResultSetService.THRESHOLD_PARAM_NAME).asInstanceOf[AnyVal]
    
    if (rawConfig.hasPath(ValidateResultSetService.POST_VALIDATION_PARAM_NAME))
      new FilterConfig(paramName, thresholdValue, rawConfig.getBoolean(ValidateResultSetService.POST_VALIDATION_PARAM_NAME))
    else
      new FilterConfig(paramName, thresholdValue)
  }

  private def parsePepMatchFilters(rawConfig: Config): Option[Seq[IPeptideMatchFilter]] = {
    if (rawConfig.hasPath(PEP_MATCH_FILTERS_PARAM)) {
      val pepMatchFiltersConfigs = rawConfig.getConfigList(PEP_MATCH_FILTERS_PARAM).toList

      Some(pepMatchFiltersConfigs.map(parseFilterConfig(_)).map(fc => {
        val nextFilter = BuildPeptideMatchFilter(fc.parameter, fc.threshold.asInstanceOf[AnyVal])
        if (fc.postValidation)
          nextFilter.setAsPostValidationFilter(true)
        nextFilter
      }).toSeq)

    } else None
  }

  private def parsePepMatchValidatorConfig(rawConfig: Config): PepMatchValidatorConfig = {
    val paramsMap = rawConfig.root().unwrapped()
    ProfiJson.getObjectMapper().convertValue(paramsMap, classOf[PepMatchValidatorConfig])
  }

  private def parsePepMatchValidator(rawConfig: Config, tdAnalyzer: Option[ITargetDecoyAnalyzer]): Option[IPeptideMatchValidator] = {
    if (rawConfig.hasPath(PEP_MATCH_VALIDATOR_CONFIG_PARAM)) {

      val pepMatchValidatorConfig = parsePepMatchValidatorConfig(rawConfig.getConfig(PEP_MATCH_VALIDATOR_CONFIG_PARAM))

      val pepMatchValidationFilter = if (pepMatchValidatorConfig.expectedFdr.isDefined) {
        BuildOptimizablePeptideMatchFilter(pepMatchValidatorConfig.parameter)
      } else {
        val currentFilter = BuildPeptideMatchFilter(pepMatchValidatorConfig.parameter, pepMatchValidatorConfig.threshold.get.asInstanceOf[AnyVal])
        currentFilter
      }

      // Build PeptideMatchValidator
      Some(
        BuildPeptideMatchValidator(
          pepMatchValidationFilter,
          pepMatchValidatorConfig.expectedFdr,
          tdAnalyzer
        )
      )

    } else None
  }

  private def parseProtSetFilters(rawConfig: Config): Option[Seq[IProteinSetFilter]] = {
    if (rawConfig.hasPath(PROT_SET_FILTERS_PARAM)) {
      val protSetFiltersConfigs = rawConfig.getConfigList(PROT_SET_FILTERS_PARAM)
      val protSetFilters = protSetFiltersConfigs.map(parseFilterConfig(_)).map(fc => BuildProteinSetFilter(fc.parameter, fc.threshold.asInstanceOf[AnyVal]))
      Some(protSetFilters)
    } else None
  }

  private def parseProtSetValidatorConfig(rawConfig: Config): ProtSetValidatorConfig = {
    ProfiJson.getObjectMapper().convertValue(rawConfig.root().unwrapped(), classOf[ProtSetValidatorConfig])
  }

  private def parseProtSetValidator(rawConfig: Config): Option[IProteinSetValidator] = {
    if (rawConfig.hasPath(PROT_SET_VALIDATOR_CONFIG_PARAM)) {

      val protSetValidatorConfig = parseProtSetValidatorConfig(rawConfig.getConfig(PROT_SET_VALIDATOR_CONFIG_PARAM))
      val thresholds = protSetValidatorConfig.thresholds.map(_.map(e => e._1 -> e._2.asInstanceOf[AnyVal]))

      // Retrieve protein set validation method
      val protSetValMethod = ProtSetValidationMethods.withName(protSetValidatorConfig.validationMethod)

      // Build ProteinSetValidator
      Some(
        BuildProteinSetValidator(
          protSetValMethod,
          protSetValidatorConfig.parameter,
          thresholds,
          protSetValidatorConfig.expectedFdr
        )
      )

    } else None
  }

}