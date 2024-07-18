package fr.proline.cli.command

import java.util.UUID

import fr.proline.cli.ProlineCommands
import fr.proline.context.IExecutionContext
import fr.proline.core.om.model.msq.AbundanceUnit
import fr.proline.core.om.model.msq.QuantMethodType
import fr.proline.core.orm.uds.Dataset
import fr.proline.core.orm.uds.Dataset.DatasetType
import fr.proline.core.orm.uds.QuantitationMethod
import fr.proline.core.orm.uds.repository.DatasetRepository
import fr.proline.module.exporter.ViewSetExporter
import fr.proline.module.exporter.commons.config.ExportConfigConstant
import fr.proline.module.exporter.commons.config.ExportConfigManager
import fr.proline.module.exporter.dataset.view.BuildDatasetViewSet

object ExportDataSet {
  
  def apply(exportDirectory: String)(implicit execCtx: IExecutionContext) {
    
    val exportLocation = new java.io.File(exportDirectory)
    val viewSetName = "proline_results_" + UUID.randomUUID().toString
    val projectId = ProlineCommands.DEFAULT_PROJECT_ID
    
    val udsEM = execCtx.getUDSDbConnectionContext.getEntityManager
    val datasets = DatasetRepository.findDatasetsByProject(udsEM, projectId).iterator()
    
    var foundDataSet: Dataset = null
    while (datasets.hasNext() && foundDataSet == null) {
      val dataset = datasets.next()
      if (dataset.getType == DatasetType.QUANTITATION) {
        foundDataSet = dataset
      }
    }
    assert(foundDataSet != null, "Can't export quantitation dataset because it has not been created")
    
    val dsId = foundDataSet.getId
    val rsmId = foundDataSet.getMasterQuantitationChannels.get(0).getQuantResultSummaryId
    
    val exportMode = DatasetUtil.getExportMode(projectId, dsId)
    val exportConfig = ExportConfigManager.getFullExportConfig(exportMode)
    
    // Create dataset export view
    val viewSet = BuildDatasetViewSet(
      executionContext = execCtx,
      projectId = projectId,
      dsId = dsId,
      rsmId = rsmId,
      viewSetName = viewSetName,
      mode = exportMode,
      exportConfig = exportConfig
    )

    ViewSetExporter.exportViewSetToDirectory(viewSet, exportLocation)
  }
  
}

// TODO: copy/pasted from proline Cotrex => move to the DataSetExporter module
object DatasetUtil {

  def getExportMode(projectId: Long, datasetId: Long)(implicit execCtx: IExecutionContext): String = {
    var mode = ExportConfigConstant.MODE_IDENT
    
    val udsDbCtx = execCtx.getUDSDbConnectionContext()
    val udsEM = udsDbCtx.getEntityManager()
    val udsDs = udsEM.find(classOf[Dataset], datasetId)
    if (udsDs != null) {
      val dsType = udsDs.getType()
      if (dsType == DatasetType.QUANTITATION) {
        mode = ExportConfigConstant.MODE_QUANT_XIC
        val dsMethod: QuantitationMethod = udsDs.getMethod()
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