package fr.proline.cortex.caller.service.dps.msi

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.ImportResultFileConfig
import fr.proline.cortex.client.service.dps.msi.IImportResultFilesServiceCaller
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

case class ImportResultFilesCLIConfig(
  projectId: Long,
  resultFiles: Seq[ResultFileDescriptorRuleId],
  instrumentConfigId: Long,
  peaklistSoftwareId: Long,
  storeSpectrumMatches: Boolean,
  importerProperties: Option[Map[String, Any]] = None
)

object ImportResultFiles {

  def apply(
    config: ImportResultFilesCLIConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Array[ImportedResultFile]] = {
    this.apply(config.projectId, ProfiJson.getObjectMapper().convertValue(config, classOf[ImportResultFileConfig]))
  }
  
  def apply(
    projectId: Long,
    config: ImportResultFileConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Array[ImportedResultFile]] = {
    val serviceCaller: IImportResultFilesServiceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msi.ImportResultFiles()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msi.ImportResultFiles()(embdCallerCtx)
      }
    }
    serviceCaller.apply(projectId, config)
  }
  
}