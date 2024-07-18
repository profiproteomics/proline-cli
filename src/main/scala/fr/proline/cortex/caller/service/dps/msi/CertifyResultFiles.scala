package fr.proline.cortex.caller.service.dps.msi

import fr.proline.cortex.api.service.dps.msi.ResultFileDescriptorRuleId
import fr.proline.cortex.caller.IServiceCall
import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.ICertifyResultFilesServiceCaller
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

case class CertifyResultFilesConfig(
  projectId: Long,
  resultFiles: Seq[ResultFileDescriptorRuleId],
  importerProperties: Option[Map[String, Any]]
)

object CertifyResultFiles {

  def apply(config: CertifyResultFilesConfig)(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    this.apply(config.projectId, config.resultFiles, config.importerProperties)
  }

  def apply(projectId: Long, resultFiles: Seq[ResultFileDescriptorRuleId], importerProperties: Option[Map[String, Any]])(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    val serviceCaller: ICertifyResultFilesServiceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msi.CertifyResultFiles()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msi.CertifyResultFiles()(embdCallerCtx)
      }
    }
    serviceCaller.apply(projectId, resultFiles, importerProperties)
  }
  
}

