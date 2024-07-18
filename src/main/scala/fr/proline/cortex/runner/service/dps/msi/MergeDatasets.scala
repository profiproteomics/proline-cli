package fr.proline.cortex.runner.service.dps.msi

import com.typesafe.scalalogging.StrictLogging

import fr.proline.core.algo.msi.AdditionMode
import fr.proline.core.service.msi.ResultSetMerger
import fr.proline.core.service.msi.ResultSummaryMerger
import fr.proline.cortex.api.service.dps.msi.MergeResultSetsServiceV2_0.RSMMergeResult
import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IMergeDatasetsServiceCaller
import fr.proline.cortex.runner._

case class MergeDatasets()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IMergeDatasetsServiceCaller with StrictLogging {
  
  def mergeResultSets(
    projectId: Long,
    resultSetIds: Seq[Long],
    additionMode: Option[String] // TODO: move AdditionMode enum from OMP to OM
  ): EmbeddedServiceCall[Long] = {

    RunEmbeddedService {
      callerCtx.inExecutionCtx(projectId, useJPA = false) { execCtx =>
        
        logger.info("ResultSet merger service will start")
        
        val rsMerger = new ResultSetMerger(
          execCtx = execCtx,
          resultSetIds = Some(resultSetIds),
          resultSets = None,
          aggregationMode = additionMode.map(AdditionMode.withName(_))
        )
  
        rsMerger.run
  
        logger.info("ResultSet merger done")
  
        rsMerger.mergedResultSet.id
      }
    }
  }
  
  def mergeResultSummaries(
    projectId: Long,
    resultSummaryIds: Seq[Long],
    additionMode: Option[String] // TODO: move AdditionMode enum from OMP to OM
  ): EmbeddedServiceCall[RSMMergeResult] = {

    RunEmbeddedService {
      callerCtx.inExecutionCtx(projectId, useJPA = false) { execCtx =>
        
        logger.info("ResultSummary merger service will start")
  
        val rsmMerger = new ResultSummaryMerger(
          execCtx = execCtx,
          resultSummaryIds = Some(resultSummaryIds),
          resultSummaries = None,
          aggregationMode = additionMode.map(AdditionMode.withName(_))
        )
        rsmMerger.run()
  
        logger.info("ResultSummary merger done")
  
        RSMMergeResult(
          targetResultSummaryId = rsmMerger.mergedResultSummary.id,
          targetResultSetId = rsmMerger.mergedResultSummary.getResultSetId
        )
      }
    }
  }

}
