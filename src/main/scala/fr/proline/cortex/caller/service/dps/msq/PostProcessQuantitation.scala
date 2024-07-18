package fr.proline.cortex.caller.service.dps.msq

import fr.proline.core.algo.msq.config.profilizer.ProfilizerConfig
import fr.proline.cortex.api.service.dps.msq._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msq.IPostProcessQuantitationServiceCaller
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

case class PostProcessQuantitationConfig(
  projectId: Long,
  masterQuantChannelId: Long,
  config: ProfilizerConfig
)

object PostProcessQuantitation {

  def apply(
    config: PostProcessQuantitationConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    this.apply(config.projectId, config.masterQuantChannelId, config.config)
  }
  
  def apply(
    projectId: Long,
    masterQuantChannelId: Long,
    config: ProfilizerConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    val serviceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msq.PostProcessQuantitation()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msq.PostProcessQuantitation()(embdCallerCtx)
      }
    }
    serviceCaller.apply(projectId, masterQuantChannelId, config)
  }
  
}
