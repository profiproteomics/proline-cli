package fr.proline.cortex.caller.service.dps.msi

import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IValidateResultSetServiceCaller
import fr.proline.cortex.client.service.dps.msi.ValidateResultSetConfig
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

object ValidateResultSet {
  
  def apply(projectId: Long, config: ValidateResultSetConfig)(implicit callerCtx: IServiceCallerContext): IServiceCall[Long] = {
    val serviceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msi.ValidateResultSet()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msi.ValidateResultSet()(embdCallerCtx)
      }
    }
    serviceCaller.apply(projectId, config)
  }
  
}
