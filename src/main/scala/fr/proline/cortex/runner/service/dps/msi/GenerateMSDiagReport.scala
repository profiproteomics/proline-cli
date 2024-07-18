package fr.proline.cortex.runner.service.dps.msi

import com.typesafe.scalalogging.LazyLogging

import fr.proline.cortex.api.service.dps.msi.IGenerateMSDiagReportService
import fr.proline.module.quality.msdiag.service.MSDiagReportGenerator

import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IGenerateMSDiagReportServiceCaller
import fr.proline.cortex.runner._

case class GenerateMSDiagReport()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IGenerateMSDiagReportServiceCaller with LazyLogging {

  def apply(
    projectId: Long,
    resultSetId: Long, 
    msdiagSettings: Map[String, Any]
  ): EmbeddedServiceCall[String] = {
    
    RunEmbeddedService {
      callerCtx.inExecutionCtx(projectId, useJPA = true) { execCtx =>
        
        val msDiagReportGenerator = new MSDiagReportGenerator(
          execCtx,
          resultSetId,
          Option(msdiagSettings)
        )
        
        msDiagReportGenerator.runService()
        
        logger.debug("GenerateMSDiagReport WS: report generated !")
    
        msDiagReportGenerator.resultHashMapJson
      }
    }

  }
  
}