package fr.proline.cortex.caller.service.dps.msq

import fr.proline.cortex.api.service.dps.msq._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msq.IQuantifySCServiceCaller
import fr.proline.cortex.client.service.dps.msq.QuantifySCConfig

object QuantifySC {
  
  def apply(
    projectId: Long,
    config: QuantifySCConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Map[String,Any]] = {
    val serviceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => fr.proline.cortex.client.service.dps.msq.QuantifySC()(jmsCallerCtx)
      case _ => throw new Exception("Not yet implemented!")
      //case embdCallerCtx: EmbeddedServiceCallerContext => fr.proline.cortex.runner.service.dps.msi.CertifyResultFiles
    }
    serviceCaller.apply(projectId, config)
  }
  
}