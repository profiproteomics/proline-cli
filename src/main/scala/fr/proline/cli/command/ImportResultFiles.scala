package fr.proline.cli.command

import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

import com.typesafe.scalalogging.LazyLogging

import fr.proline.cli.DbConnectionHelper
import fr.proline.cli.ProlineCommands
import fr.proline.cli.config.ImportResultFilesConfig
import fr.proline.context.IExecutionContext
import fr.proline.core.dal.helper.UdsDbHelper
import fr.proline.core.om.provider.ProviderDecoratedExecutionContext
import fr.proline.core.om.provider.msi._
import fr.proline.core.om.provider.msi.impl.SQLPTMProvider
import fr.proline.core.service.msi.ResultFileImporter
import fr.proline.cortex.api.service.dps.msi.ImportedResultFile

import fr.proline.cortex.api.service.dps.msi.ResultFileDescriptorsDecoyRegExp

object ImportResultFiles extends LazyLogging {
  
  def apply(inputFileList: String, configFileOpt: Option[String]) {
    
    val importConfig = if (configFileOpt.isEmpty) ImportResultFilesConfig()
    else ImportResultFilesConfig.loadAndParseConfig(configFileOpt.get)
    
    val resultFiles = scala.io.Source.fromFile(inputFileList).getLines.toArray.map { line =>
      
      val fileExt = new File(line).getName.split("\\.").last.toLowerCase()
      
      // Infer the file format given the file name
      val fileFormat = fileExt match {
        case "dat" => "mascot.dat"
        case "mzid" => "mzidentml.mzid"
        case "omx" => "omssa.omx"
        case "xml" => "xtandem.xml"
        case _ => "unknown." + fileExt
      }
      
      ResultFileDescriptorsDecoyRegExp(
        format = fileFormat,
        path = line,
        decoyStrategy = importConfig.decoyRegex
      )
    }
    
    val projectId = importConfig.projectId.getOrElse(ProlineCommands.DEFAULT_PROJECT_ID)
    val instrumentConfigId = importConfig.instrumentConfigId.getOrElse(ProlineCommands.DEFAULT_INSTRUMENT_ID)
    val peaklistSoftwareId = importConfig.peaklistSoftwareId.getOrElse(ProlineCommands.DEFAULT_PEAKLIST_SOFTWARE_ID)
    /*val importerProperties = if (params.hasParam(IMPORTER_PROPERTIES_PARAM) == false) Map.empty[String, Any]
    else params.getMap(IMPORTER_PROPERTIES_PARAM).map {
      case (a, b) => {
        if (a.endsWith(".file")) {
          a -> MountPointRegistry.replacePossibleLabel(b.toString(), Some(MountPointRegistry.RESULT_FILES_DIRECTORY)).localPathname
        } else a -> b.asInstanceOf[Any]
      }
    } toMap*/

    val storeSpectrumMatches = importConfig.storeSpectrumMatches

    // Initialize the providers    
    val execCtx = DbConnectionHelper.createSQLExecutionContext(projectId)

    val result = try {
      val parserCtx = this._buildParserContext(execCtx)
      val udsDbCtx = execCtx.getUDSDbConnectionContext()

      val udsDbHelper = new UdsDbHelper(udsDbCtx)

      val importedRFs = new ArrayBuffer[ImportedResultFile]

      for (resultFile <- resultFiles) {
        val resultFileType = resultFile.format
        val importedRF = new ImportedResultFile(resultFile.path)

        // Instantiate the appropriate result file provider and register it
        val rfProviderOpt = ResultFileProviderRegistry.get(resultFileType)
        assert(rfProviderOpt.isDefined, "unsupported result file type: " + resultFileType)

        val rfProvider = rfProviderOpt.get

        val acDecoyRegex = resultFile.decoyStrategy.map(new Regex(_))
        if (acDecoyRegex.isDefined) {
          this.logger.info(s"Parsing concatenated decoy results with AC regex=[${acDecoyRegex.get}]")
        }

        // TODO: what to do about mount points ???
        //val localPathname = MountPointRegistry.replacePossibleLabel(resultFile.path, Some(MountPointRegistry.RESULT_FILES_DIRECTORY)).localPathname
        val localPathName = resultFile.path
        
        // Instantiate the ResultFileImporter service
        val rsImporter = new ResultFileImporter(
          executionContext = parserCtx,
          resultIdentFile = new File(localPathName),
          fileType = rfProvider.fileType,
          instrumentConfigId = instrumentConfigId,
          peaklistSoftwareId = peaklistSoftwareId,
          // TODO: set importerProperties
          importerProperties = Map(), //importerProperties,
          acDecoyRegex = acDecoyRegex,
          storeSpectraData = false,
          storeSpectrumMatches = storeSpectrumMatches
        )
        rsImporter.run()

        importedRF.targetResultSetId = rsImporter.getTargetResultSetId

        importedRFs += importedRF
      }
      
      /* Return result */
      importedRFs.toArray

    } finally {
      DbConnectionHelper.tryToCloseExecContext(execCtx)
    }

  }
  
  private def _buildParserContext(executionContext: IExecutionContext): ProviderDecoratedExecutionContext = {

    // Register some providers
    val parserContext = ProviderDecoratedExecutionContext(executionContext) // Use Object factory

    // TODO: use real protein and seqDb providers
    parserContext.putProvider(classOf[IProteinProvider], ProteinFakeProvider)
    parserContext.putProvider(classOf[ISeqDatabaseProvider], SeqDbFakeProvider)

    val msiSQLCtx = executionContext.getMSIDbConnectionContext
    val sqlPTMProvider = new SQLPTMProvider(msiSQLCtx)
    parserContext.putProvider(classOf[IPTMProvider], sqlPTMProvider)

    parserContext.putProvider(classOf[IPeptideProvider], PeptideFakeProvider)
    /*val sqlPepProvider = new SQLPeptideProvider(psSQLCtx)
    parserContext.putProvider(classOf[IPeptideProvider], sqlPepProvider)*/

    parserContext
  }
}