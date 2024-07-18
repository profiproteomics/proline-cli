package fr.proline.cortex.runner.service.dps.msi

import com.typesafe.scalalogging.LazyLogging

import fr.proline.context.DatabaseConnectionContext
import fr.proline.context.IExecutionContext
import fr.proline.core.dal.context._
import fr.proline.core.service.msi.OrphanDataDeleter
import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IDeleteOrphanDataServiceCaller
import fr.proline.cortex.runner._

case class DeleteOrphanData()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IDeleteOrphanDataServiceCaller with LazyLogging  {

  def deleteResultSets(
    projectId: Long,
    resultSetIds: Seq[Long]
  ): EmbeddedServiceCall[Boolean] = {
    this._deleteOrphanData(projectId, resultSetIds.toArray, Array.emptyLongArray)
  }

  def deleteResultSummaries(
    projectId: Long,
    resultSummaryIds: Seq[Long]
  ): EmbeddedServiceCall[Boolean] = {
    this._deleteOrphanData(projectId, Array.emptyLongArray, resultSummaryIds.toArray)
  }
  
  private def _deleteOrphanData(
    projectId: Long,
    resultSetIds: Array[Long],
    resultSummaryIds: Array[Long]
  ): EmbeddedServiceCall[Boolean] = {
    
    RunEmbeddedService {
    
      callerCtx.inExecutionCtx(projectId, useJPA = true) { execCtx =>
        execCtx.tryInTransactions(msiTx = true, txWork = {
          val orphanDataDeleter = new OrphanDataDeleter(execCtx, projectId, resultSummaryIds, resultSetIds)
          orphanDataDeleter.runService()
        })
      }
     
      true
    }
  }

}