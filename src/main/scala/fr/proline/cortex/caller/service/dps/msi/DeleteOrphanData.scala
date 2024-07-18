package fr.proline.cortex.caller.service.dps.msi

import fr.proline.cortex.caller.IServiceCall
import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IDeleteOrphanDataServiceCaller
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

case class DeleteOrphanResultSetsConfig(projectId: Long, resultSetIds: Seq[Long])
case class DeleteOrphanResultSummariesConfig(projectId: Long, resultSummaryIds: Seq[Long])

object DeleteOrphanData {
  
  def apply()(implicit callerCtx: IServiceCallerContext): IDeleteOrphanDataServiceCaller = {
    val serviceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msi.DeleteOrphanData()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msi.DeleteOrphanData()(embdCallerCtx)
      }
    }
    serviceCaller
  }

  def deleteResultSets(
    config: DeleteOrphanResultSetsConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    this.apply().deleteResultSets(config.projectId, config.resultSetIds)
  }

  def deleteResultSummaries(
    config: DeleteOrphanResultSummariesConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    this.apply().deleteResultSummaries(config.projectId, config.resultSummaryIds)
  }
  
}