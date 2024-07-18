package fr.proline.cortex.caller.service.dps.msi

import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client._
import fr.proline.cortex.caller._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IFilterRSMProteinSetsServiceCaller
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

object FilterRSMProteinSets {
  
  def apply(
    projectId: Long,
    resultSummaryId: Long,
    protSetFilterConfigs: Seq[FilterConfig]
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    val serviceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msi.FilterRSMProteinSets()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msi.FilterRSMProteinSets()(embdCallerCtx)
      }
    }
    serviceCaller.apply(projectId, resultSummaryId, protSetFilterConfigs)
  }
  
}