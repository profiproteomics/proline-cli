package fr.proline.cortex.runner.service.dps.msi

import java.io.File
import java.util.concurrent.Executors

import scala.collection.JavaConversions.mapAsScalaMap
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.matching.Regex

import com.typesafe.scalalogging.LazyLogging

import fr.proline.context.IExecutionContext
import fr.proline.core.dal.helper.UdsDbHelper
import fr.proline.core.om.provider.PeptideCacheExecutionContext
import fr.proline.core.om.provider.ProviderDecoratedExecutionContext
import fr.proline.core.om.provider.msi._
import fr.proline.core.om.provider.msi.impl.SQLPTMProvider
import fr.proline.core.om.provider.msi.impl.SQLPeptideProvider
import fr.proline.core.service.msi.ResultFileImporter
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IImportResultFilesServiceCaller
import fr.proline.cortex.client.service.dps.msi.ImportResultFileConfig
import fr.proline.cortex.runner._

object ImportResultFiles {
  val execCtxForImport = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
}

case class ImportResultFiles()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IImportResultFilesServiceCaller with LazyLogging  {
  
  import fr.proline.core.om.provider.ProlineManagedDirectoryType
  import fr.proline.config.MountPointPathConverter
  
  private lazy val mountPointPathConverter = new MountPointPathConverter(callerCtx.mountPointRegistry)
  
  private def _mountPointBasedPathToLocalPath(path: String): String = {
    mountPointPathConverter.prolinePathToAbsolutePath(path, ProlineManagedDirectoryType.RESULT_FILES)
  }
  
  def apply(
    projectId: Long,
    config: ImportResultFileConfig
  ): EmbeddedServiceCall[Array[ImportedResultFile]] = {

    val futureImport = Future {
      val resultFiles = config.resultFiles
      val instrumentConfigId = config.instrumentConfigId
      val peaklistSoftwareId = config.peaklistSoftwareId
      val importerProperties = config.importerProperties
      val storeSpectrumMatches = config.storeSpectrumMatches
      
      val updatedImporterProperties = if (importerProperties.isEmpty) Map.empty[String, Any]
      // FIXME: DBO => please, comment what is performed here
      else importerProperties.get.map { case (a, b) =>
        a -> (if (!a.endsWith(".file")) b else _mountPointBasedPathToLocalPath(b.toString))
      }
  
      callerCtx.inExecutionCtx(projectId, useJPA = false) { execCtx =>
  
        val parserCtx = this._buildParserContext(execCtx)
        val udsDbCtx = execCtx.getUDSDbConnectionContext()
  
        val udsDbHelper = new UdsDbHelper(udsDbCtx)
        val decoyRegexById = udsDbHelper.getProteinMatchDecoyRegexById()
  
        val importedRFs = new collection.mutable.ArrayBuffer[ImportedResultFile]
  
        for (resultFile <- resultFiles) {
          val resultFileType = resultFile.format
          val importedRF = new ImportedResultFile(resultFile.path)
  
          // Retrieve the appropriate result file provider
          val rfProviderOpt = ResultFileProviderRegistry.get(resultFileType)
          require(
            rfProviderOpt.isDefined,
            "Unsupported result file type: " + resultFileType
          )
  
          val rfProvider = rfProviderOpt.get
  
          val acDecoyRegexOpt = resultFile.protMatchDecoyRuleId.map(decoyRegexById(_))
          if (acDecoyRegexOpt.isDefined) {
            this.logger.info(s"Parsing concatenated decoy results with AC regex=[${acDecoyRegexOpt.get}]")
          }
  
          val localPathname = _mountPointBasedPathToLocalPath(resultFile.path)
          
          // Instantiate the ResultFileImporter service
          val rsImporter = new ResultFileImporter(
            executionContext = parserCtx,
            resultIdentFile = new File(localPathname),
            fileType = rfProvider.fileType,
            instrumentConfigId = instrumentConfigId,
            peaklistSoftwareId = peaklistSoftwareId,
            importerProperties = updatedImporterProperties,
            acDecoyRegex = acDecoyRegexOpt,
            storeSpectrumMatches = storeSpectrumMatches
          )
          rsImporter.run()
  
          importedRF.targetResultSetId = rsImporter.getTargetResultSetId
  
          importedRFs += importedRF
        }

        importedRFs.toArray
      }
    } (ImportResultFiles.execCtxForImport)
    
    RunEmbeddedService(futureImport)
  }
  
  private def _buildParserContext(executionContext: IExecutionContext): ProviderDecoratedExecutionContext = {

    // Register some providers
    val parserContext = ProviderDecoratedExecutionContext(executionContext) // Use Object factory
    //parserContext.putProvider(classOf[IPeptideProvider], PeptideFakeProvider)

    // TODO: use real protein and seqDb providers
    parserContext.putProvider(classOf[IProteinProvider], ProteinFakeProvider)
    parserContext.putProvider(classOf[ISeqDatabaseProvider], SeqDbFakeProvider)

    val msiSQLCtx = executionContext.getMSIDbConnectionContext
    val sqlPTMProvider = new SQLPTMProvider(msiSQLCtx)
    parserContext.putProvider(classOf[IPTMProvider], sqlPTMProvider)

    val sqlPepProvider = new SQLPeptideProvider(new PeptideCacheExecutionContext(executionContext))
    parserContext.putProvider(classOf[IPeptideProvider], sqlPepProvider)

    parserContext
  }

}
