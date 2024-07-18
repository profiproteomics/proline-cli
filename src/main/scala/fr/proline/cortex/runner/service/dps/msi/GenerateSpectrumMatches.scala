package fr.proline.cortex.runner.service.dps.msi

import com.typesafe.scalalogging.LazyLogging

import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.GenerateSpectrumMatchesConfig
import fr.proline.cortex.client.service.dps.msi.IGenerateSpectrumMatchesServiceCaller
import fr.proline.cortex.runner._
import fr.proline.module.fragmentmatch.service.SpectrumMatchesGenerator

case class GenerateSpectrumMatches()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IGenerateSpectrumMatchesServiceCaller with LazyLogging {

  def apply(
    projectId: Long, 
    config: GenerateSpectrumMatchesConfig
  ): EmbeddedServiceCall[Boolean] = {
    
    RunEmbeddedService {
    
      callerCtx.inExecutionCtx(projectId, useJPA = true) { execCtx =>
        val spectrumMatchesGenerator = new SpectrumMatchesGenerator(
          execCtx,
          config.resultSetId,
          config.resultSummaryId,
          config.peptideMatchIds,
          config.fragmentationRuleSetId,
          config.forceInsert.getOrElse(false)
        )
        
        spectrumMatchesGenerator.runService()
      }
    }
  }
    
}