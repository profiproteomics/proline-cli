package fr.proline.cortex.runner.service.dps.msi

import com.typesafe.scalalogging.LazyLogging

import fr.proline.core.algo.msi.InferenceMethod
import fr.proline.core.algo.msi.filtering.IPeptideMatchFilter
import fr.proline.core.algo.msi.filtering.IPeptideMatchSorter
import fr.proline.core.algo.msi.filtering.IProteinSetFilter
import fr.proline.core.algo.msi.filtering.pepmatch.ScorePSMFilter
import fr.proline.core.algo.msi.scoring.PepSetScoring
import fr.proline.core.algo.msi.validation.BasicTDAnalyzer
import fr.proline.core.algo.msi.validation.BuildOptimizablePeptideMatchFilter
import fr.proline.core.algo.msi.validation.BuildPeptideMatchFilter
import fr.proline.core.algo.msi.validation.BuildPeptideMatchValidator
import fr.proline.core.algo.msi.validation.BuildProteinSetFilter
import fr.proline.core.algo.msi.validation.BuildProteinSetValidator
import fr.proline.core.algo.msi.validation.IPeptideMatchValidator
import fr.proline.core.algo.msi.validation.IProteinSetValidator
import fr.proline.core.algo.msi.validation.ITargetDecoyAnalyzer
import fr.proline.core.algo.msi.validation.ProtSetValidationMethods
import fr.proline.core.algo.msi.validation.TargetDecoyModes
import fr.proline.core.dal.context._
import fr.proline.core.service.msi.ResultSetValidator
import fr.proline.core.service.msi.ValidationConfig
import fr.proline.cortex.api.service.dps.msi.FilterConfig
import fr.proline.cortex.api.service.dps.msi.IValidateResultSetService
import fr.proline.cortex.api.service.dps.msi.PepMatchValidatorConfig
import fr.proline.cortex.api.service.dps.msi.ProtSetValidatorConfig
import fr.proline.cortex.api.service.dps.msi.ValidateResultSetService

import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IValidateResultSetServiceCaller
import fr.proline.cortex.client.service.dps.msi.IValidateResultSetConfig
import fr.proline.cortex.client.service.dps.msi.ValidateResultSetConfig
import fr.proline.cortex.runner._

case class ValidateResultSet()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IValidateResultSetServiceCaller {
  
  def apply(
    projectId: Long,
    config: ValidateResultSetConfig
  ): EmbeddedServiceCall[Long] = {
    
    RunEmbeddedService {
      callerCtx.inExecutionCtx(projectId, useJPA = false) { execCtx =>
  
        /*
        // Use peptide match validator as sorter if provided, else use default ScorePSM         
        val sorter: IPeptideMatchSorter =
          if (config.pepMatchValidatorConfig.isDefined
            && config.pepMatchValidator.get.validationFilter.isInstanceOf[IPeptideMatchSorter])
            validationConfig.pepMatchValidator.get.validationFilter.asInstanceOf[IPeptideMatchSorter]
          else if (validationConfig.pepMatchPreFilters.isDefined) {
            var foundSorter: IPeptideMatchSorter = null
            var index = 0
            while (foundSorter == null && index < validationConfig.pepMatchPreFilters.get.size) {
              if (validationConfig.pepMatchPreFilters.get(index).isInstanceOf[IPeptideMatchSorter]) {
                foundSorter = validationConfig.pepMatchPreFilters.get(index).asInstanceOf[IPeptideMatchSorter]
              }
              index = index +1 
            }
            if (foundSorter == null)
              foundSorter = new ScorePSMFilter()
            foundSorter
          } else {
            new ScorePSMFilter()
          }
        */
        
        val validationConfig = this.buildValidationConfig(config)
  
        // Instantiate a result set validator
        val rsValidator = ResultSetValidator(
          execContext = execCtx,
          targetRsId = config.resultSetId,
          tdAnalyzer = validationConfig.tdAnalyzer,
          pepMatchPreFilters = validationConfig.pepMatchPreFilters,
          pepMatchValidator = validationConfig.pepMatchValidator,
          protSetFilters = validationConfig.protSetFilters,
          protSetValidator = validationConfig.protSetValidator,
          inferenceMethod = Some(InferenceMethod.PARSIMONIOUS),
          peptideSetScoring = Some(validationConfig.pepSetScoring.getOrElse(PepSetScoring.MASCOT_STANDARD_SCORE))
        )
  
        execCtx.getMSIDbConnectionContext.tryInTransaction {
          rsValidator.run()
        }
  
        rsValidator.validatedTargetRsm.id
        
      } // ends inExecutionCtx
    }
  }
  
  def buildValidationConfig(validateRsConfig: IValidateResultSetConfig): ValidationConfig = {
    
    val pepMatchFilters = this._buildPepMatchFilters(validateRsConfig.pepMatchFilters)
    
    // Create a default TD analyzer 
    val tdAnalyzer = Some(new BasicTDAnalyzer(TargetDecoyModes.CONCATENATED))
    val pepMatchValidator = _buildPepMatchValidator(validateRsConfig.pepMatchValidatorConfig, tdAnalyzer)
    val pepSetScoring = validateRsConfig.pepSetScoreType.map(PepSetScoring.withName(_))
    val protSetFilters = _buildProtSetFilters(validateRsConfig.protSetFilters)
    val protSetValidator = _buildProtSetValidator(validateRsConfig.protSetValidatorConfig)

    ValidationConfig(
      tdAnalyzer = None,
      pepMatchPreFilters = pepMatchFilters,
      pepMatchValidator = pepMatchValidator,
      pepSetScoring = pepSetScoring,
      protSetFilters = protSetFilters,
      protSetValidator = protSetValidator
    )
  }
  
  private def _buildPepMatchFilters(filterConfigs: Option[Array[FilterConfig]]): Option[Seq[IPeptideMatchFilter]] = {
    if (filterConfigs.isEmpty) None
    else {
      val pepMatchFilters = filterConfigs.get.map { fc =>
        val filter = BuildPeptideMatchFilter(fc.parameter, fc.threshold)
        if (fc.postValidation)
          filter.setAsPostValidationFilter(true)
          
        filter
      }
      
      Some(pepMatchFilters)
    }
    
  }
  
  private def _buildPepMatchValidator(pepMatchValidatorConfigOpt: Option[PepMatchValidatorConfig], tdAnalyzer: Option[ITargetDecoyAnalyzer]): Option[IPeptideMatchValidator] = {
    if (pepMatchValidatorConfigOpt.isEmpty) None
    else {
      
      val pepMatchValidatorConfig = pepMatchValidatorConfigOpt.get

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
    }
  }
  
  private def _buildProtSetFilters(filterConfigs: Option[Array[FilterConfig]]): Option[Seq[IProteinSetFilter]] = {
    if (filterConfigs.isEmpty) None
    else {
      Some(filterConfigs.get.map(fc => BuildProteinSetFilter(fc.parameter, fc.threshold)).toSeq)
    }
  }
  
  private def _buildProtSetValidator(protSetValidatorConfigOpt: Option[ProtSetValidatorConfig]): Option[IProteinSetValidator] = {
    if (protSetValidatorConfigOpt.isEmpty) None
    else {

      val protSetValidatorConfig = protSetValidatorConfigOpt.get
      val thresholds = protSetValidatorConfig.thresholds

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
    }
  }

}
