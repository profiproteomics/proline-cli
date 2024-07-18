package fr.proline.cortex.caller.service.dps.msi

import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IUpdateSpectraParamsServiceCaller
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

object UpdateSpectraParams {
  
  def apply(
    projectId: Long,
    resultSetIds: Seq[Long],
    peaklistSoftwareId: Long
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Int] = {
    val serviceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msi.UpdateSpectraParams()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msi.UpdateSpectraParams()(embdCallerCtx)
      }
    }
    serviceCaller.apply(projectId, resultSetIds, peaklistSoftwareId)
  }
  
}