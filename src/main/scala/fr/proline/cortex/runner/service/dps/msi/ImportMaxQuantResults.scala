package fr.proline.cortex.runner.service.dps.msi

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.util.zip.ZipInputStream

import scala.collection.mutable.HashMap

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FilenameUtils

import fr.proline.context.IExecutionContext
import fr.proline.core.om.provider.PeptideCacheExecutionContext
import fr.proline.core.om.provider.ProviderDecoratedExecutionContext
import fr.proline.core.om.provider.msi._
import fr.proline.core.om.provider.msi.impl.SQLPTMProvider
import fr.proline.core.om.provider.msi.impl.SQLPeptideProvider
import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IImportMaxQuantResultsServiceCaller
import fr.proline.cortex.runner._
import fr.proline.module.parser.maxquant.MaxQuantResultParser

case class ImportMaxQuantResults()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IImportMaxQuantResultsServiceCaller with LazyLogging {
  
  import fr.proline.core.om.provider.ProlineManagedDirectoryType
  import fr.proline.config.MountPointPathConverter
  
  private lazy val mountPointPathConverter = new MountPointPathConverter(callerCtx.mountPointRegistry)
  
  private def _mountPointBasedPathToLocalPath(path: String): String = {
    mountPointPathConverter.prolinePathToAbsolutePath(path, ProlineManagedDirectoryType.RESULT_FILES)
  }

  def apply(
    projectId: Long,
    resultFilesDir: String,
    instrumentConfigId: Long,
    peaklistSoftwareId: Long
  ): EmbeddedServiceCall[collection.Map[String, Any]] = {
    
    RunEmbeddedService {
      
      callerCtx.inExecutionCtx(projectId, useJPA = false) { execCtx =>
        
        val parserCtx = this._buildParserContext(execCtx)
        val udsDbCtx = execCtx.getUDSDbConnectionContext()
        val localPathname = _mountPointBasedPathToLocalPath(resultFilesDir)
  
        val localFile = new File(localPathname)
        require(
          localFile.exists(),
          s"Specified Path not found on server side: $resultFilesDir"
        )
  
        val mqFolderName = if (localFile.isDirectory()) localPathname
        else {//Should be a zip file !
          unZipIt(localFile)
        }
        logger.debug(s"Try to Import MaxQuant result file located at: $mqFolderName")
  
        // Instantiate the ResultFileImporter service
        val rsMQImporter = new MaxQuantResultParser(
          parserCtx,
          instrumentConfigId,
          peaklistSoftwareId,
          mqFolderName
        )
        rsMQImporter.run()
        
        val rsIds = rsMQImporter.getCreatedResultSetIds
        val serviceResult = new HashMap[String, Any]()
        // FIXME: it should be result_set_ids instead of result_set_Ids
        // TODO: create a case class instead of building a HashMap manually
        serviceResult.put("result_set_Ids", rsIds)
        serviceResult.put("warning_msg", rsMQImporter.getWarningMessage)
       
        logger.debug(s"MaxQuant results successfully imported: $serviceResult")
        
        serviceResult
      }
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

  /**
   * Unzip maxQuant result file
   * @param zipFile input zip file
   * @param output output folder
   * @return the root folder name
   */
  private def unZipIt(zipFile: File): String = {

    val buffer = new Array[Byte](4096)
    var zis: ZipInputStream = null
    try {

      // Create output directory 
      val parentFolder = zipFile.getParentFile
      val folderName = FilenameUtils.getBaseName(zipFile.getAbsolutePath)
      val folder = new File(parentFolder, folderName + "_unzip")
      if (!folder.exists()) {
        folder.mkdir()
      }

      // Get the zip file content
      zis = new ZipInputStream(new FileInputStream(zipFile))
      // Get the zipped file list entry
      var ze = zis.getNextEntry()

      while (ze != null) {

        val nextFileName = ze.getName()
        val newFile = new File(folder, nextFileName)
        logger.debug("file unzip : " + newFile.getAbsoluteFile())

        // Create all non exists folders
        // Else you will hit FileNotFoundException for compressed folder
        new File(newFile.getParent()).mkdirs()

        val fos = new FileOutputStream(newFile)
        var len = zis.read(buffer)
        while (len > 0) {
          fos.write(buffer, 0, len)
          len = zis.read(buffer)
        }
        fos.close()

        ze = zis.getNextEntry()
      } // ends while (ze != null)

      zis.closeEntry()
      zis.close()

      logger.debug(s"ZIP file ${zipFile.getName()} successfully unzipped!")

      if (folder.list().length == 1) //root folder in ZIP
        return folder.listFiles().head.getAbsolutePath
      else
        return folder.getAbsolutePath

    } finally {
      if (zis != null) {
        try {
          zis.closeEntry()
          zis.close()
        } catch {
          case e: IOException => logger.error("Error closing ZIP : " + zipFile.getName)
        }
      }
    }

  }

}

