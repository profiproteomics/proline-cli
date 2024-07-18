package fr.proline.cortex.caller.service.dps.msi

import fr.proline.cortex.api.service.dps.msi.{ExportFileFormat, ExportResultSummaryIdentifier}
import fr.proline.cortex.caller.{IServiceCall, IServiceCallerContext}
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IExportResultSummariesServiceCaller
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

case class ExportResultSummariesConfig(
  rsmIdentifiers: Seq[ExportResultSummaryIdentifier],
  fileFormat: ExportFileFormat.Value,
  fileDirectory: String,
  fileName: String,
  extraParams: Option[Map[String, Any]] = None
)

object ExportResultSummaries {
  
  private def _apply()(implicit callerCtx: IServiceCallerContext): IExportResultSummariesServiceCaller = {
    val serviceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msi.ExportResultSummaries()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msi.ExportResultSummaries()(embdCallerCtx)
      }
    }

    serviceCaller
  }

  def apply(
    config: ExportResultSummariesConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Array[String]] = {
    this._apply().export(
      config.rsmIdentifiers, config.fileFormat, config.fileDirectory, config.fileName, config.extraParams
    )
  }

  def apply(
    rsmIdentifiers: Seq[ExportResultSummaryIdentifier],
    fileFormat: ExportFileFormat.Value,
    fileDirectory: String,
    fileName: String,
    extraParams: Option[Map[String, Any]] = None
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Array[String]] = {
    this._apply().export(
      rsmIdentifiers, fileFormat, fileDirectory, fileName, extraParams
    )
  }
  
}

