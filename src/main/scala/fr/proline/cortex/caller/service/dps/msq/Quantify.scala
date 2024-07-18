package fr.proline.cortex.caller.service.dps.msq

import fr.profi.util.serialization.ProfiJson
import fr.proline.core.om.model.msq.ExperimentalDesign
import fr.proline.cortex.caller._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msq.IQuantifyServiceCaller
import fr.proline.cortex.client.service.dps.msq.QuantifyConfig
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

case class QuantifyCLIConfig(
  projectId: Long,
  name: String,
  description: String,
  methodId: Long,
  experimentalDesign: ExperimentalDesign,
  quantitationConfig: AnyRef // classes extending the IQuantConfig model
)

object Quantify {

  def apply(
    config: QuantifyCLIConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Long] = {
    this.apply(config.projectId, ProfiJson.getObjectMapper().convertValue(config, classOf[QuantifyConfig]))
  }
  
  def apply(
    projectId: Long,
    config: QuantifyConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Long] = {
    val serviceCaller: IQuantifyServiceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msq.Quantify()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msq.Quantify()(embdCallerCtx)
      }
    }
    serviceCaller.apply(projectId, config)
  }
  
}
