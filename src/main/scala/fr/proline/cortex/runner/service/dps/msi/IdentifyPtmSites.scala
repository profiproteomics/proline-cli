/*package fr.proline.cortex.runner.service.dps.msi

import com.typesafe.scalalogging.LazyLogging

import fr.proline.context.DatabaseConnectionContext
import fr.proline.core.algo.msi.filtering.IProteinSetFilter
import fr.proline.core.service.msi.RsmPtmSitesIdentifier

import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IIdentifyPtmSitesService
import fr.proline.cortex.runner._

class IdentifyPtmSites extends AbstractRemoteProcessingService with IIdentifyPtmSitesService with LazyLogging {

  def doProcess(paramsRetriever: NamedParamsRetriever): Any = {

    require(paramsRetriever != null, "no parameter specified")

    val projectId = paramsRetriever.getLong(PROCESS_METHOD.PROJECT_ID_PARAM)
    val resultSummaryId = paramsRetriever.getLong(PROCESS_METHOD.RESULT_SUMMARY_ID_PARAM)
    val force = if (paramsRetriever.hasParam(PROCESS_METHOD.FORCE_PARAM)) { paramsRetriever.getBoolean(PROCESS_METHOD.FORCE_PARAM) } else { false }
    
    val execCtx = DbConnectionHelper.createJPAExecutionContext(projectId)

    var msiDbConnectionContext: DatabaseConnectionContext = null

    try {
        msiDbConnectionContext = execCtx.getMSIDbConnectionContext
        msiDbConnectionContext.beginTransaction()

        // Instantiate a Ptm sites identifier
        val ptmSitesIdentifier = new RsmPtmSitesIdentifier(
          execContext = execCtx,
          resultSummaryId = resultSummaryId,
          force = force
        )

        ptmSitesIdentifier.run

        //Commit transaction
        msiDbConnectionContext.commitTransaction()
    } catch {
      case t: Throwable => {
        throw ExceptionUtils.wrapThrowable("Error while identifying Ptm sites", t, appendCause = true)
      }
    } finally {
      DbConnectionHelper.tryToCloseExecContext(execCtx)
    }

    true
  }
}*/