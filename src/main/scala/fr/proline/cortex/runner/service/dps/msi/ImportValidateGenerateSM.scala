package fr.proline.cortex.runner.service.dps.msi

import java.io.File
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

import com.typesafe.scalalogging.LazyLogging

import fr.profi.util.StringUtils
import fr.proline.context.DatabaseConnectionContext
import fr.proline.context.IExecutionContext
import fr.proline.core.algo.msi.InferenceMethod
import fr.proline.core.algo.msi.scoring.PepSetScoring
import fr.proline.core.dal.context._
import fr.proline.core.dal.helper.UdsDbHelper
import fr.proline.core.om.model.msi.ResultSet
import fr.proline.core.om.provider.PeptideCacheExecutionContext
import fr.proline.core.om.provider.ProviderDecoratedExecutionContext
import fr.proline.core.om.provider.msi._
import fr.proline.core.om.provider.msi.impl.SQLPTMProvider
import fr.proline.core.om.provider.msi.impl.SQLPeptideProvider
import fr.proline.core.orm.uds.IdentificationDataset
import fr.proline.core.orm.uds.Project
import fr.proline.core.orm.uds.repository.DatasetRepository
import fr.proline.core.service.msi.ResultFileCertifier
import fr.proline.core.service.msi.ResultFileImporter
import fr.proline.core.service.msi.ResultSetValidator
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.module.fragmentmatch.service.SpectrumMatchesGenerator

import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi._
import fr.proline.cortex.runner._

case class ImportValidateGenerateSM()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IImportValidateGenerateSMServiceCaller with LazyLogging {
  
  import fr.proline.core.om.provider.ProlineManagedDirectoryType
  import fr.proline.config.MountPointPathConverter
  
  private lazy val mountPointPathConverter = new MountPointPathConverter(callerCtx.mountPointRegistry)
  
  private def _mountPointBasedPathToLocalPath(path: String): String = {
    mountPointPathConverter.prolinePathToAbsolutePath(path, ProlineManagedDirectoryType.RESULT_FILES)
  }
  
  def apply(
    projectId: Long,
    
    // Import specific parameters
    importResultFileConfig: ImportResultFileConfig,
    
    // Validation specific parameters
    validateResultSetConfig: ValidateResultSetConfig,
    
    // "Generate Spectrum Matches" specific parameters
    generateSpectrumMatches: Option[Boolean] = None,
    forceInsert: Option[Boolean] = None // TODO: rename this parameter

  ): EmbeddedServiceCall[Array[ImportedResultFile]] = {
    
    RunEmbeddedService {
      
      val resultFiles = importResultFileConfig.resultFiles
      val instrumentConfigId = importResultFileConfig.instrumentConfigId
      val peaklistSoftwareId = importResultFileConfig.peaklistSoftwareId
      val importerProperties = importResultFileConfig.importerProperties
      val storeSpectrumMatches = importResultFileConfig.storeSpectrumMatches
      val genSpecMatches = generateSpectrumMatches.getOrElse(false)
      val gsmForceInsert = forceInsert.getOrElse(false)
      
      val updatedImporterProperties = if (importerProperties.isEmpty) Map.empty[String, Any]
      // FIXME: DBO => please, comment what is performed here
      else importerProperties.get.map { case (a, b) =>
        a -> (if (!a.endsWith(".file")) b else _mountPointBasedPathToLocalPath(b.toString))
      }
      
      val importedRFs = new ArrayBuffer[ImportedResultFile]
      
      callerCtx.inExecutionCtx(projectId, useJPA = true) { execCtx =>
        execCtx.tryInTransactions(msiTx = true, udsTx = true, txWork = {
          
          val parserCtx = this._buildParserContext(execCtx)
          val msiDbConnectionContext = execCtx.getMSIDbConnectionContext
          val udsbConnectionContext = execCtx.getUDSDbConnectionContext()
    
          val udsDbHelper = new UdsDbHelper(udsbConnectionContext)
          val decoyRegexById = udsDbHelper.getProteinMatchDecoyRegexById()
    
          // Group result files by file format
          // TODO: do this in the certifier service ?
          val filesByFormat = resultFiles.toArray.groupBy(_.format).mapValues { rfs =>
            rfs.map(rf => new File(_mountPointBasedPathToLocalPath(rf.path)))
          }
  
          val errorMessage = new StringBuilder()
  
          // **** Certify result files **** //
          val rsCertifier = new ResultFileCertifier(
            executionContext = parserCtx,
            resultIdentFilesByFormat = filesByFormat,
            importProperties = updatedImporterProperties
          )
          rsCertifier.run()
  
          // **** Iterate result files **** //
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
    
            // **** Import Result File **** //
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
    
            val currentRSId = rsImporter.getTargetResultSetId 
            val currentRSOPt =  rsImporter.getTargetResultSetOpt
            val currentRS = currentRSOPt.get
            importedRF.targetResultSetId = currentRSId
            importedRFs += importedRF
  
            /* Determine dataset name */
            val dsName = if (currentRS.msiSearch.isEmpty) currentRS.name
            else {
              val name = currentRS.msiSearch.get.resultFileName
              name.split('.').headOption.getOrElse(name)
            }
            
            //*** Create Identification Dataset for new RS
            // Then insert it in the current MSIdb
            val udsEM = udsbConnectionContext.getEntityManager
            val project = udsEM.find(classOf[Project], projectId)
            val ds = new IdentificationDataset()
            ds.setProject(project)
            ds.setName(dsName)
            ds.setResultSetId(currentRSId)
            ds.setChildrenCount(0)
            
            val rootDsNames = DatasetRepository.findRootDatasetNamesByProject(udsEM,projectId)
            val dsNbr = if (rootDsNames == null) 0 else rootDsNames.size()
            ds.setNumber(dsNbr)
            
            val validationConfig = ValidateResultSet().buildValidationConfig(validateResultSetConfig)
            
            // **** Validate Result Set
            val rsValidator = new ResultSetValidator(
              execContext = execCtx,
              targetRs = currentRS,
              tdAnalyzer = validationConfig.tdAnalyzer,
              pepMatchPreFilters = validationConfig.pepMatchPreFilters,
              pepMatchValidator = validationConfig.pepMatchValidator,
              protSetFilters = validationConfig.protSetFilters,
              protSetValidator = validationConfig.protSetValidator,
              inferenceMethod = Some(InferenceMethod.PARSIMONIOUS),
              peptideSetScoring = Some(validationConfig.pepSetScoring.getOrElse(PepSetScoring.MASCOT_STANDARD_SCORE))
            )
            rsValidator.run()   
            
            val currentRSMId = rsValidator.validatedTargetRsm.id
            
            // Complete DS info and persist it
            ds.setResultSummaryId(currentRSMId)
            udsEM.persist(ds)
            
            if(genSpecMatches) {
              val spectrumMatchesGenerator = new SpectrumMatchesGenerator(
                execCtx,
                currentRSId,
                Some(currentRSMId),
                None,
                None,
                gsmForceInsert
              )
              spectrumMatchesGenerator.runService()
            }
            
          } // End Go through RS
  
        }) // ends tryInTransactions
      }
      
      importedRFs.toArray
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

    val sqlPepProvider = new SQLPeptideProvider(new PeptideCacheExecutionContext(executionContext))
    parserContext.putProvider(classOf[IPeptideProvider], sqlPepProvider)

    parserContext
  }

}

