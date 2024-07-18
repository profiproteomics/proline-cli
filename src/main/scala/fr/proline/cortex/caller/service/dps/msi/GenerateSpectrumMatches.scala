package fr.proline.cortex.caller.service.dps.msi

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.GenerateSpectrumMatchesConfig
import fr.proline.cortex.client.service.dps.msi.IGenerateSpectrumMatchesServiceCaller
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

case class GenerateSpectrumMatchesCLIConfig(
  projectId: Long,
  resultSetId: Long,
  resultSummaryId: Option[Long],
  peptideMatchIds: Option[Array[Long]],
  fragmentationRuleSetId: Option[Long],
  forceInsert: Option[Boolean] = None
)

object GenerateSpectrumMatches {

  def apply(
    config: GenerateSpectrumMatchesCLIConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    this.apply(config.projectId, ProfiJson.getObjectMapper().convertValue(config, classOf[GenerateSpectrumMatchesConfig]))
  }
  
  def apply(
    projectId: Long,
    config: GenerateSpectrumMatchesConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    
    val serviceCaller: IGenerateSpectrumMatchesServiceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msi.GenerateSpectrumMatches()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msi.GenerateSpectrumMatches()(embdCallerCtx)
      }
    }
    serviceCaller.apply(projectId, config)
  }
  
}
