package fr.proline.cortex.runner.service.dps.msi

import java.sql.Connection

import com.typesafe.scalalogging.LazyLogging

import fr.proline.core.dal.tables.SelectQueryBuilder.any2ClauseAdd
import fr.proline.core.dal.tables.SelectQueryBuilder1
import fr.proline.core.dal.tables.SelectQueryBuilder2
import fr.proline.core.dal.tables.msi.MsiDbMsiSearchTable
import fr.proline.core.dal.tables.msi.MsiDbResultSetTable
import fr.proline.core.dal.tables.uds.UdsDbPeaklistSoftwareTable
import fr.proline.core.dal.DoJDBCReturningWork
import fr.proline.core.service.msi.SpectraParamsUpdater
import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IUpdateSpectraParamsServiceCaller
import fr.proline.cortex.runner._

case class UpdateSpectraParams()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IUpdateSpectraParamsServiceCaller with LazyLogging {
  
  def apply(
    projectId: Long,
    resultSetIds: Seq[Long],
    peaklistSoftwareId: Long
  ): EmbeddedServiceCall[Int] = {

    RunEmbeddedService {
      callerCtx.inExecutionCtx(projectId, useJPA = true) { execCtx =>
        
        // Get peaklist ids for specified result sets
        // TODO: move to MsiDbHelper
        // TODO: what about child peaklists
        val msiDbCtx = execCtx.getMSIDbConnectionContext
        val peaklistIds = DoJDBCReturningWork.withEzDBC(msiDbCtx) { ezDBC =>
          val queryStr = new SelectQueryBuilder2(MsiDbResultSetTable,MsiDbMsiSearchTable).mkSelectQuery { (t1,c1,t2,c2) =>
  					List(t2.PEAKLIST_ID) -> "WHERE "~ t1.MSI_SEARCH_ID ~" = "~ t2.ID ~" AND "~ t1.ID ~ s" IN (${resultSetIds.mkString(",")})"
          }
          ezDBC.selectLongs(queryStr)
        }
        
        // Get spectrum title parsing rule id for specified peaklist software
        // TODO: move to MsiDbHelper
        val udsDbCtx = execCtx.getUDSDbConnectionContext
        val specTitleParsingRuleIdOpt = DoJDBCReturningWork.withEzDBC(udsDbCtx) { ezDBC =>
          val queryStr = new SelectQueryBuilder1(UdsDbPeaklistSoftwareTable).mkSelectQuery { (t,c) =>
  					List(t.SPEC_TITLE_PARSING_RULE_ID) -> "WHERE "~ t.ID ~ s" = $peaklistSoftwareId"
          }
          ezDBC.selectHeadOption(queryStr) { _.nextLong }
        }
  
        require(
          specTitleParsingRuleIdOpt.isDefined,
          "No spectrum parsing rule found for peaklist software with id="+peaklistSoftwareId
        )
        
        logger.info("UpdateSpectraParams service is starting...")
  
        val updatedSpectraCount = SpectraParamsUpdater.updateSpectraParams(
          execCtx,
          projectId,
          peaklistIds,
          specTitleParsingRuleIdOpt.get
        )
  
        logger.info(s"$updatedSpectraCount spectra updated!")
        
        updatedSpectraCount
      } // ends inExecutionCtx
    }
  }
}