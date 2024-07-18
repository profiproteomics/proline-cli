package fr.proline.cortex.runner.service.dps.msi

import java.io.File

import com.typesafe.scalalogging.LazyLogging

import fr.profi.util.StringUtils
import fr.proline.context.IExecutionContext
import fr.proline.core.om.provider.PeptideCacheExecutionContext
import fr.proline.core.om.provider.ProviderDecoratedExecutionContext
import fr.proline.core.om.provider.msi._
import fr.proline.core.om.provider.msi.impl.SQLPTMProvider
import fr.proline.core.om.provider.msi.impl.SQLPeptideProvider
import fr.proline.core.service.msi.ResultFileCertifier
import fr.proline.cortex.api.service.dps.msi.ResultFileDescriptorRuleId

import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.ICertifyResultFilesServiceCaller
import fr.proline.cortex.runner._

case class CertifyResultFiles()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends ICertifyResultFilesServiceCaller with LazyLogging {
  
  import fr.proline.core.om.provider.ProlineManagedDirectoryType
  import fr.proline.config.MountPointPathConverter
  
  private lazy val mountPointPathConverter = new MountPointPathConverter(callerCtx.mountPointRegistry)
  
  private def _mountPointBasedPathToLocalPath(path: String): String = {
    mountPointPathConverter.prolinePathToAbsolutePath(path, ProlineManagedDirectoryType.RESULT_FILES)
  }
  
  def apply(projectId: Long, resultFiles: Seq[ResultFileDescriptorRuleId], importerProperties: Option[Map[String, Any]]): EmbeddedServiceCall[Boolean] = {
    
    RunEmbeddedService {
      val updatedImporterProperties = if (importerProperties.isEmpty) Map.empty[String, Any]
      // FIXME: DBO => please, comment what is performed here
      else importerProperties.get.map { case (a, b) =>
        a -> (if (!a.endsWith(".file")) b else _mountPointBasedPathToLocalPath(b.toString))
      }
  
      val certifierResult = callerCtx.inExecutionCtx(projectId, useJPA = true) { execCtx =>
        
        val parserCtx = _buildParserContext(execCtx)
  
        // Group result files by file format
        // TODO: do this in the certifier service ?
        val filesByFormat = resultFiles.toArray.groupBy(_.format).mapValues { rfs =>
          rfs.map(rf => new File(_mountPointBasedPathToLocalPath(rf.path)))
        }
  
        var areFilesOK: Boolean = false
        val errorMessage = new StringBuilder()
        
        try {
        
      	  // Instantiate the ResultFileCertifier service
      	  val rsCertifier = new ResultFileCertifier(
            executionContext = parserCtx,
            resultIdentFilesByFormat = filesByFormat,
            importProperties = updatedImporterProperties
          )
  
        	rsCertifier.run()      	
        
        	areFilesOK = true
        	
        } catch {
          case i : InterruptedException => {
            errorMessage.append("Service was interrupted")
            throw i
          }
          case t: Throwable => {
            val message = "Error certifying ResultFiles"
  
            logger.error(message, t)
  
            errorMessage.append(message).append(" : ").append(t)
            errorMessage.append(StringUtils.LINE_SEPARATOR)
  
            errorMessage.append(t.getStackTrace.mkString("\n"))
          }
        }
         
        if (areFilesOK) {
          "OK" // ResultFileCertifier success
        } else {
          errorMessage.toString // ResultFileCertifier complete abruptly
        }
  
      }
      
      certifierResult == "OK"
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

