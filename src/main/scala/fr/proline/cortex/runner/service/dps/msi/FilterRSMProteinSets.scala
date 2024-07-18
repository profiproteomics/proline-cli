package fr.proline.cortex.runner.service.dps.msi

import com.typesafe.scalalogging.LazyLogging

import fr.proline.context.DatabaseConnectionContext
import fr.proline.core.algo.msi.filtering.IProteinSetFilter
import fr.proline.core.algo.msi.validation.BuildProteinSetFilter
import fr.proline.core.dal.context._
import fr.proline.core.service.msi.RsmProteinSetFilterer
import fr.proline.cortex.api.service.dps.msi.FilterConfig
import fr.proline.cortex.api.service.dps.msi.IFilterRSMProteinSetsService

import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IFilterRSMProteinSetsServiceCaller
import fr.proline.cortex.runner._

case class FilterRSMProteinSets()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IFilterRSMProteinSetsServiceCaller with LazyLogging {
  
  def apply(
    projectId: Long,
    resultSummaryId: Long,
    protSetFilterConfigs: Seq[FilterConfig]
  ): EmbeddedServiceCall[Boolean] = {
    
    RunEmbeddedService {
    
      val protSetFilters = protSetFilterConfigs.map { fc =>
        logger.debug(s"Try to build protein set filter for param=${fc.parameter} and threshold=${fc.threshold}")
        BuildProteinSetFilter(fc.parameter, fc.threshold)
      }
      
      callerCtx.inExecutionCtx(projectId, useJPA = true) { execCtx =>
        execCtx.tryInTransactions(msiTx = true, txWork = {
          RsmProteinSetFilterer(
            execCtx = execCtx,
            targetRsmId = resultSummaryId,
            protSetFilters = protSetFilters
          ).run()
        })
      }
     
      true
    }
  }

}