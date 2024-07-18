package fr.proline.cortex.caller.service.dps.msi

import fr.proline.cortex.caller._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IImportMaxQuantResultsServiceCaller
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

case class ImportMaxQuantResultsConfig(
  projectId: Long,
  resultFilesDir: String,
  instrumentConfigId: Long,
  peaklistSoftwareId: Long
)

object ImportMaxQuantResults {

  def apply(
    config: ImportMaxQuantResultsConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[collection.Map[String, Any]] = {
    this.apply(config.projectId, config.resultFilesDir, config.instrumentConfigId, config.peaklistSoftwareId)
  }
  
  def apply(
    projectId: Long,
    resultFilesDir: String,
    instrumentConfigId: Long,
    peaklistSoftwareId: Long
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[collection.Map[String, Any]] = {
    val serviceCaller: IImportMaxQuantResultsServiceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msi.ImportMaxQuantResults()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msi.ImportMaxQuantResults()(embdCallerCtx)
      }
    }
    serviceCaller.apply(projectId, resultFilesDir, instrumentConfigId, peaklistSoftwareId)
  }
  
}
