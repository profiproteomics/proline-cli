package fr.proline.cortex.caller.service.dps.msi

import fr.proline.cortex.caller._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IGenerateMSDiagReportServiceCaller
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

case class GenerateMSDiagReportConfig(
  projectId: Long,
  resultSetId: Long,
  msdiagSettings: Map[String, Any]
)

object GenerateMSDiagReport {

  def apply(config: GenerateMSDiagReportConfig)(implicit callerCtx: IServiceCallerContext): IServiceCall[String] = {
    this.apply(config.projectId, config.resultSetId, config.msdiagSettings)
  }
  
  def apply(
    projectId: Long,
    resultSetId: Long,
    msdiagSettings: Map[String, Any]
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[String] = {

    val serviceCaller: IGenerateMSDiagReportServiceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msi.GenerateMSDiagReport()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msi.GenerateMSDiagReport()(embdCallerCtx)
      }
    }

    serviceCaller.apply(projectId, resultSetId, msdiagSettings)
  }
  
}
