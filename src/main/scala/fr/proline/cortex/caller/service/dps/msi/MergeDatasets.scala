package fr.proline.cortex.caller.service.dps.msi

import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.client._
import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IMergeDatasetsServiceCaller
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

object MergeDatasets {
  
  def apply()(implicit callerCtx: IServiceCallerContext): IMergeDatasetsServiceCaller = {
    callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msi.MergeDatasets()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msi.MergeDatasets()(embdCallerCtx)
      }
    }
  }
  
}

