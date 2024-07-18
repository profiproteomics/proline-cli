package fr.proline.cortex.runner.service.dps.msi

import java.io.File
import java.util.HashMap
import java.util.UUID

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.LazyLogging

import fr.profi.util.StringUtils
import fr.profi.util.serialization.ProfiJson
import fr.proline.context.IExecutionContext
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.module.exporter.ViewSetExporter
import fr.proline.module.exporter.commons.config._
import fr.proline.module.exporter.dataset.view.BuildDatasetViewSet
import fr.proline.module.exporter.msi.template.SpectraListAsTSV
import fr.proline.module.exporter.msi.view.{BuildRSMSpectraViewSet, FormatCompatibility}
import fr.proline.module.exporter.mzidentml.MzIdExporter
import fr.proline.module.exporter.mzidentml.model.Contact
import fr.proline.module.exporter.mzidentml.model.Organization
import fr.proline.module.exporter.pridexml.PrideExporterService
import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IExportResultSummariesServiceCaller
import fr.proline.cortex.runner._

case class ExportResultSummaries()(implicit val callerCtx: EmbeddedServiceCallerContext) extends IExportResultSummariesServiceCaller with LazyLogging {

  def export(
    rsmIdentifiers: Seq[ExportResultSummaryIdentifier],
    fileFormat: ExportFileFormat.Value,
    fileDirectory: String,
    fileName: String,
    extraParams: Option[Map[String, Any]] = None
  ): EmbeddedServiceCall[Array[String]] = {
    
    RunEmbeddedService {
      val filePaths = fileFormat match {
        case ExportFileFormat.MZIDENTML    => exportToMzIdentML(rsmIdentifiers.head, fileName, fileDirectory, extraParams)(callerCtx)
        case ExportFileFormat.TEMPLATED    => exportToTemplatedFile(rsmIdentifiers, fileName, fileDirectory, extraParams)(callerCtx)
        case ExportFileFormat.PRIDE        => exportToPrideFile(rsmIdentifiers.head, fileName, fileDirectory, extraParams)(callerCtx)
        case ExportFileFormat.SPECTRA_LIST => exportToSpectraList(rsmIdentifiers.head, fileName, fileDirectory, extraParams)(callerCtx)
      }
    
      filePaths
    }
  }
  
  protected def exportToMzIdentML(
    rsmIdentifier: ExportResultSummaryIdentifier,
    fileName: String,
    fileDir: String,
    extraParams: Option[Map[String, Any]] = None
  )(callerCtx: EmbeddedServiceCallerContext): Array[String] = {

    callerCtx.inExecutionCtx(rsmIdentifier.projectId, useJPA = true) { execCtx =>

      val exporter = new MzIdExporter(rsmIdentifier.rsmId, execCtx)

      val filePath = fileDir + File.separatorChar + fileName + ".mzid"

      val objectMapper = ProfiJson.getObjectMapper()

      val contactParamOpt = extraParams.flatMap(_.get("contact"))
      val exportContact = if(contactParamOpt.isEmpty)  new Contact("Proline User", "ProFI", None, None)
      else {
        objectMapper.convertValue(
          contactParamOpt.get,
          classOf[fr.proline.module.exporter.mzidentml.model.Contact]
        )
      }

      val orgParamOpt = extraParams.flatMap(_.get("organization"))
      val exportOrg = if(orgParamOpt.isEmpty) new Organization("Proline User", None)
      else {
        objectMapper.convertValue(
          orgParamOpt.get,
          classOf[fr.proline.module.exporter.mzidentml.model.Organization]
        )
      }

      exporter.exportResultSummary(filePath, exportContact, exportOrg)
      
      Array(filePath)
    }
  }

  protected def exportToPrideFile(
    rsmIdentifier: ExportResultSummaryIdentifier,
    fileName: String,
    fileDir: String,
    extraParams: Option[Map[String, Any]] = None
  )(callerCtx: EmbeddedServiceCallerContext): Array[String] = {

    val exportLocation = new java.io.File(fileDir)
    val filePath = fileDir + File.separator + fileName

    callerCtx.inExecutionCtx(rsmIdentifier.projectId, useJPA = false) { execCtx =>

      val extraParamsMap = extraParams.get.mapValues(_.asInstanceOf[Object])
      
      val exporter = new PrideExporterService(
        execCtx,
        rsmIdentifier.rsmId,
        filePath,
        extraParamsMap.toMap
      )
      exporter.runService()
    }

    Array(filePath)
  }

  protected def exportToTemplatedFile(
    rsmIdentifiers: Seq[ExportResultSummaryIdentifier],
    fileName: String,
    fileDir: String,
    extraParams: Option[Map[String, Any]] = None
  )(callerCtx: EmbeddedServiceCallerContext): Array[String] = {
    require(extraParams.isDefined, "some extra parameters must be provided")

    val projectId = rsmIdentifiers(0).projectId
    
    callerCtx.inExecutionCtx(projectId, useJPA = true) { implicit execCtx =>
      
      val mode = DatasetUtil.getExportMode(
        rsmIdentifiers(0).projectId,
        rsmIdentifiers(0).dsId.get
      )
  
      val exportConfig = if (extraParams != null && extraParams.isDefined && extraParams.get.contains("config")) {
        ExportConfigManager.readConfig(extraParams.get("config").asInstanceOf[String])
      } else {
        // default conf if not filled -- all dataset have same type, so the config is based on the first
        ExportConfigManager.getDefaultExportConfig(mode)
      }
  
      val exportLocation = new java.io.File(fileDir)
      val exportedFiles = new ArrayBuffer[String]()
  
      for (rsmIdentifier <- rsmIdentifiers) {
  
        var fileDatasetName = fileName
        if (StringUtils.isEmpty(fileName)) {
          fileDatasetName = "DatasetSummaryExport-" + rsmIdentifier.dsId.get + "_" + UUID.randomUUID().toString //In this version of exported DSId is mandatory
        }

        // Export
        val viewSet = BuildDatasetViewSet(
          executionContext = execCtx,
          projectId = rsmIdentifier.projectId,
          dsId = rsmIdentifier.dsId.get,
          rsmId = rsmIdentifier.rsmId,
          viewSetName = fileDatasetName,
          mode = mode,
          exportConfig = exportConfig
        )

        val exFiles = ViewSetExporter.exportViewSetToDirectory(viewSet, exportLocation)
        for (f <- exFiles) {
          exportedFiles += f.getAbsolutePath
        }
      }
      
      exportedFiles.toArray
    }

  }

  protected def exportToSpectraList(
    rsmIdentifier: ExportResultSummaryIdentifier,
    fileName: String,
    fileDir: String,
    extraParams: Option[Map[String, Any]] = None
  )(callerCtx: EmbeddedServiceCallerContext): Array[String] = {
    
    val formatCompatibility = if (extraParams != null && extraParams.isDefined && extraParams.get.contains("format_compatibility")) {
      FormatCompatibility.withName(extraParams.get("format_compatibility").asInstanceOf[String])
    } else {
      FormatCompatibility.PEAKVIEW
    }

    callerCtx.inExecutionCtx(rsmIdentifier.projectId, useJPA = true) { execCtx =>

      // Export
      val viewSet = BuildRSMSpectraViewSet(
        execCtx,
        rsmIdentifier.projectId,
        rsmIdentifier.rsmId,
        viewSetName = fileName,
        viewSetTemplate = SpectraListAsTSV,
        mode = formatCompatibility
      )
      
      val exportedFiles = ViewSetExporter.exportViewSetToDirectory(viewSet, new File(fileDir))
      
      exportedFiles.map(_.getAbsolutePath).toArray
    }

  }
}

// TODO: copy/pasted from proline Cotrex => move to the DataSetExporter module
object DatasetUtil {

  import fr.proline.core.om.model.msq.AbundanceUnit
  import fr.proline.core.om.model.msq.QuantMethodType

  def getExportMode(projectId: Long, datasetId: Long)(implicit execCtx: IExecutionContext): String = {
    var mode = ExportConfigConstant.MODE_IDENT
    
    val udsDbCtx = execCtx.getUDSDbConnectionContext()
    val udsEM = udsDbCtx.getEntityManager()
    val udsDs = udsEM.find(classOf[fr.proline.core.orm.uds.Dataset], datasetId)
    if (udsDs != null) {
      val dsType = udsDs.getType()
      if (dsType == fr.proline.core.orm.uds.Dataset.DatasetType.QUANTITATION) {
        mode = ExportConfigConstant.MODE_QUANT_XIC
        val dsMethod = udsDs.getMethod()
        val quantMethodType = dsMethod.getType
        val abundanceUnit = dsMethod.getAbundanceUnit
        if (quantMethodType == QuantMethodType.LABEL_FREE.toString() && abundanceUnit == AbundanceUnit.SPECTRAL_COUNTS.toString()) {
          mode = ExportConfigConstant.MODE_QUANT_SC
        }
        else if (abundanceUnit == AbundanceUnit.REPORTER_ION_INTENSITY.toString()) {
          mode = ExportConfigConstant.MODE_QUANT_TAGGING
        }
      }
    }
      
    mode
  }
  
}