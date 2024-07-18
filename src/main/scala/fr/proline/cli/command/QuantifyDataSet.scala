package fr.proline.cli.command

import java.io.File
import javax.persistence.EntityManager

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.LongMap

import com.typesafe.scalalogging.LazyLogging

import fr.profi.util.StringUtils
import fr.proline.cli.ProlineCommands
import fr.proline.cli.config.QuantifyDataSetConfig
import fr.proline.context.IExecutionContext
import fr.proline.core.dal.context._
import fr.proline.core.om.model.msq._
import fr.proline.core.om.provider.ProviderDecoratedExecutionContext
import fr.proline.core.om.provider.lcms.IRunProvider
import fr.proline.core.om.provider.lcms.impl.SQLRunProvider
import fr.proline.core.om.provider.lcms.impl.SQLScanSequenceProvider
import fr.proline.core.orm.uds.RawFile
import fr.proline.core.orm.uds.Run
import fr.proline.core.orm.uds.QuantitationMethod.{ Type => UdsQuantMethodType }
import fr.proline.core.orm.uds.repository.QuantitationMethodRepository
import fr.proline.core.service.uds.Quantifier
import fr.proline.admin.service.db.SetupProline

object QuantifyDataSet extends LazyLogging {
  
  def registerMzDbFile(mzDbFilePath: String)(implicit execCtx: IExecutionContext): RawFile = {
    val rawFileIdentifier = new File(mzDbFilePath).getName.split("""\.""").head
    val overwriteRawFile = true
    
    // Retrieve UdsDb context and entity manager
    val udsDbCtx = execCtx.getUDSDbConnectionContext
    val udsEM = udsDbCtx.getEntityManager
    
    // Search for this raw file identifier in the UDSdb
    val existingRawFile = udsEM.find(classOf[RawFile], rawFileIdentifier)
    
    // Create new raw file
    val udsRawFile = if (existingRawFile != null) {
      if (overwriteRawFile == false) {
        logger.info(s"The raw file '$rawFileIdentifier' is already registered, but no update will be performed ('overwrite' option set to false) !")
        //return existingRawFile.getRuns.get(0).getId.longValue()
        return existingRawFile
      } else {
        logger.warn(s"The raw file '$rawFileIdentifier' is already registered, it's properties (location, owner...) will be updated !")
        existingRawFile
      }
    }
    else {
      val newRawFile = new RawFile()
      newRawFile.setIdentifier(rawFileIdentifier)
      
      newRawFile
    }
    
    logger.info(s"Registering mzDB file: $mzDbFilePath")
  
    // FIXME: retrieve correct IDs
    udsRawFile.setOwnerId(1L)
    
    // Parse mzDB file meta-data
    this._extractMzDbFileMetaData(udsRawFile, mzDbFilePath)

    val rawFilePath = mzDbFilePath.substring(0, mzDbFilePath.lastIndexOf('.'))
    val rawFileFake = new java.io.File(rawFilePath)
    udsRawFile.setRawFileName(rawFileFake.getName)
      
    // Retrieve the run id
    if (existingRawFile == null) {
      udsEM.persist(udsRawFile)
      val udsRun = this._attachRunToRawFile(udsRawFile, udsEM)
      val runs = new java.util.ArrayList[Run]()
      runs.add(udsRun)
      udsRawFile.setRuns(runs)
    }

    udsRawFile
  }
  
  private def _attachRunToRawFile(udsRawFile: RawFile, udsEM: EntityManager): Run = {

    // Create new run and link it to the raw file
    val udsRun = new Run()
    udsRun.setNumber(1)
    udsRun.setRunStart(0f)
    udsRun.setRunStop(0f)
    udsRun.setDuration(0f)
    udsRun.setRawFile(udsRawFile)

    udsEM.persist(udsRun)
    
    udsRun
  }

  private def _extractMzDbFileMetaData(udsRawFile: RawFile, mzDbFilePath: String): Unit = {

    val mzDbFile = new java.io.File(mzDbFilePath)
    val mzDbFileDir = mzDbFile.getParent()
    val mzDbFileName = mzDbFile.getName()
    udsRawFile.setMzDbFileDirectory(mzDbFileDir)
    udsRawFile.setMzDbFileName(mzDbFileName)

    //val mzDbFileLocalPathname = MountPointRegistry.replacePossibleLabel(mzDbFilePath).localPathname
    //val mzDbFileLocal = new java.io.File(mzDbFileLocalPathname)
    val mzDbFileLocal = new File(mzDbFilePath)

    if (mzDbFileLocal.exists) {

      val mzDb = new fr.profi.mzdb.MzDbReader(mzDbFileLocal, false)

      try {
        // Retrieve and set the sample name
        val sampleName = mzDb.getSamples().get(0).getName()
        udsRawFile.setSampleName(sampleName)

        // Retrieve and set the raw file creation date from the mzDB file if not already set
        if (udsRawFile.getCreationTimestamp() == null) {
          val creationDate = mzDb.getRuns().get(0).getStartTimestamp()
          if (creationDate != null) {
            val creationDateAsEpochMilli = creationDate.getTime()
            udsRawFile.setCreationTimestamp(new java.sql.Timestamp(creationDateAsEpochMilli))
          }

        }

      } finally {
        mzDb.close()
      }
    }

    ()
  }

  
  def detectPeakels() {
    
  }
  
  case class ExpDesignRow(values: Array[String]) {
  
    val valuesCount = values.length
    require(valuesCount == 2 || valuesCount == 3, s"Invalid number of values to build ExpDesignRow: $valuesCount (should be 2 or 3).")
  
    val inputFileName: String = values(0)
    val bioGroup: String = values(1)
    val bioSample: Option[String] = if (valuesCount == 3) Some(values(2)) else None
  }

  /** Parse a text file containing tab-separated columns to get rows defining the experimental design **/
  private def _parseExpDesignTextFile(filePath: String): Seq[ExpDesignRow] = {

    /* Test file existence */
    require(!StringUtils.isEmpty(filePath),"No file provided for experimental design")
    require(new File(filePath).exists(), s"Can't find file at $filePath")
    
    val rowBuffer = new ArrayBuffer[ExpDesignRow]()
    
    /* Read file line by line */
    val lines = scala.io.Source.fromFile(filePath).getLines

    /* Get information from first line */
    val firstLine = lines.next()
    val firstLineCellCount = firstLine.split("\t").length
    require(
      firstLineCellCount == 2 || firstLineCellCount == 3,
      s"Incorrect number of cells in first line ($firstLine). It should be 2 or 3. See the help)"
    )

    /* Build exp design from all rows */
    lines foreach { line =>
      val cells = line.split("\t")

      val cellCount = cells.length
      require(
        cellCount == firstLineCellCount,
        s"Incorrect number of cells in line '$line'. Should be $firstLineCellCount (see the help)"
      )

      rowBuffer += ExpDesignRow(cells)
    }
    
    rowBuffer
  }
  
  private def _rowsToExpDesign(
    expDesignRows: Array[ExpDesignRow],
    rsmIdByRawFileIdent: collection.Map[String,Long],
    runIdByRawFileIdent: collection.Map[String,Long]
  ): ExperimentalDesign = {
    
    val bioGroupNumberByName = new HashMap[String,Int]
    val sampleNumberByName = new HashMap[String,Int]
    val sampleNumbersByBioGroupNumber = new LongMap[ArrayBuffer[Int]]
    
    var qcNumber = 0
    val quantChannels = expDesignRows.map { expDesignRow =>
      qcNumber += 1
      
      val bioGroupName = expDesignRow.bioGroup
      val bioGroupCount = bioGroupNumberByName.keySet.size
      val bioGroupNumber = bioGroupNumberByName.getOrElseUpdate(bioGroupName,bioGroupCount + 1)
      
      val bioSampleNameOpt = expDesignRow.bioSample
      val bioSampleNumber = if (bioSampleNameOpt.isDefined) {
        val bioSampleCount = sampleNumberByName.keySet.size
        sampleNumberByName.getOrElseUpdate(bioGroupName,bioSampleCount + 1)
      } else {
        sampleNumberByName.getOrElseUpdate(bioGroupName,bioGroupNumber)
      }
      
      sampleNumbersByBioGroupNumber.getOrElseUpdate(bioGroupNumber,new ArrayBuffer[Int]) += bioSampleNumber
      
      val rawFileIdent = new File(expDesignRow.inputFileName).getName.split("\\.").head
      val rsmId = rsmIdByRawFileIdent(rawFileIdent)
      val runId = runIdByRawFileIdent(rawFileIdent)
      
      QuantChannel(
        id = -qcNumber,
        number = qcNumber,
        name = rawFileIdent,
        sampleNumber = bioSampleNumber,
        identResultSummaryId = rsmId,
        runId = Some(runId)
      )
    }
    
    val masterQuantChannel = MasterQuantChannel(
      id = -1,
      number = 1,
      quantChannels = quantChannels
    )
    
    val bioGroups = bioGroupNumberByName.map { case (bioGroupName,bioGroupNumber) =>
      val sampleNumbers = sampleNumbersByBioGroupNumber(bioGroupNumber).distinct.sorted
      BiologicalGroup(
        id = -bioGroupNumber,
        number = bioGroupNumber,
        name = bioGroupName,
        sampleNumbers = sampleNumbers.toArray
      )
    }
    
    val bioSamples = sampleNumberByName.map { case (bioSampleName,bioSampleNumber) =>
      BiologicalSample(
        id = -bioSampleNumber,
        number = bioSampleNumber,
        name = bioSampleName
      )
    }
    
    ExperimentalDesign(
      biologicalSamples = bioSamples.toArray,
      groupSetups = Array(
        GroupSetup(
          id = -1,
          number = 1,
          name = "default group setup",
          biologicalGroups = bioGroups.toArray
        )
      ),
      masterQuantChannels = Array(masterQuantChannel)
    )
  }
  
  def apply(expDesignFile: String, configFile: String)(implicit execCtx: IExecutionContext) {
    
    /*DoJDBCWork.withConnection(execCtx.getUDSDbConnectionContext) { conn =>
      conn match {
        case sqliteConn: SQLiteConnection  => {
          println("this is an sqlite connection")
          //((SQLiteConnection)con).setBusyTimeout(35000);
        }
      }
    }*/
    
    // Parse experimental design file
    val expDesignRows = this._parseExpDesignTextFile(expDesignFile)
    
    // Register mzDB files
    execCtx.getUDSDbConnectionContext.beginTransaction()
    val udsRawFiles = expDesignRows.map(_.inputFileName).map { mzDbFilePath =>
      this.registerMzDbFile(mzDbFilePath)
    }
    /*if (execCtx.getUDSDbConnectionContext.isInTransaction()) {
      execCtx.getUDSDbConnectionContext.getEntityManager.flush()
    }*/
    execCtx.getUDSDbConnectionContext.commitTransaction()
    // FIXME: we make a clear because runs are not well mapped to raw files
    //execCtx.getUDSDbConnectionContext.getEntityManager.clear()
    
    
    val quantifyConfig = QuantifyDataSetConfig.loadAndParseConfig(configFile)
    
    val projectId = quantifyConfig.projectId.getOrElse(ProlineCommands.DEFAULT_PROJECT_ID)
    val quantConfigAsMap = quantifyConfig.quantitationConfig.get

    // Retrieve the run id mapped by the raw file identifier
    val runIdByRawFileIdent = new HashMap[String,Long]()
    udsRawFiles.foreach { udsRawFile =>
      runIdByRawFileIdent += udsRawFile.getIdentifier -> udsRawFile.getRuns.get(0).getId
    }
    /*val runIdByRawFileIdent = new HashMap[String,Long]()
    val udsEM = execCtx.getUDSDbConnectionContext.getEntityManager
    
    val udsRawFileCriteria = udsEM.getCriteriaBuilder().createQuery(classOf[fr.proline.core.orm.uds.RawFile])
    udsRawFileCriteria.select(udsRawFileCriteria.from(classOf[fr.proline.core.orm.uds.RawFile]))
    val udsRawFiles = udsEM.createQuery(udsRawFileCriteria).getResultList().iterator()
    while (udsRawFiles.hasNext()) {
      val udsRawFile = udsRawFiles.next()
      runIdByRawFileIdent += udsRawFile.getIdentifier -> udsRawFile.getRuns.get(0).getId
    }*/
    
    val rsmIdByRawFileIdent = new HashMap[String,Long]()
    val msiEM = execCtx.getMSIDbConnectionContext.getEntityManager
    val msiRsmCriteria = msiEM.getCriteriaBuilder().createQuery(classOf[fr.proline.core.orm.msi.ResultSummary])
    msiRsmCriteria.select(msiRsmCriteria.from(classOf[fr.proline.core.orm.msi.ResultSummary]))
    val msiRsms = msiEM.createQuery(msiRsmCriteria).getResultList().iterator()
    
    while (msiRsms.hasNext()) {
      val msiRsm = msiRsms.next()
      val msiMsiSearch = msiRsm.getResultSet.getMsiSearch
      if (msiMsiSearch != null) {
        // FIXME: there is a small bug in the rawFileIdentifier parser
        var rawFileIdentifier = msiRsm.getResultSet.getMsiSearch.getPeaklist.getRawFileIdentifier
        // Workaround for issue described above
        if (rawFileIdentifier.contains('\\')) {
          rawFileIdentifier = rawFileIdentifier.split('\\').last
        }
        
        rsmIdByRawFileIdent += (rawFileIdentifier -> msiRsm.getId)
      }
    }
    
    val expDesign = this._rowsToExpDesign(expDesignRows.toArray, rsmIdByRawFileIdent, runIdByRawFileIdent)
    
    // Register SQLRunProvider 
    val scanSeqProvider = new SQLScanSequenceProvider(execCtx.getLCMSDbConnectionContext())
    val lcMsRunProvider = new SQLRunProvider(
      execCtx.getUDSDbConnectionContext(),
      Some(scanSeqProvider),
      None //Some(MountPointPathConverter) // TODO: do we need this ?
    )
    val providerContext = ProviderDecoratedExecutionContext(execCtx) // Use Object factory
    providerContext.putProvider(classOf[IRunProvider], lcMsRunProvider)
    
    // TODO: remove me, temporary workarounds used until configuration files have been revised (see #15945,#15948)
    /*fr.proline.core.service.lcms.io.ExtractMapSet.setMzdbMaxParallelism(
      NodeConfig.MZDB_MAX_PARALLELISM
    )
    fr.proline.core.service.lcms.io.ExtractMapSet.setTempDirectory(
      new java.io.File(NodeConfig.PEAKELDB_TEMP_DIRECTORY)
    )*/
    
    fr.proline.core.service.lcms.io.PeakelsDetector.setTempDirectory(
      SetupProline.config.dataDirectoryOpt.get
    )
    
    val udsEM = execCtx.getUDSDbConnectionContext.getEntityManager
    
    // FIXME: quant method name should not be null
    println("quant method name: " + quantifyConfig.quantMethodName)
    
    val udsQuantMethod = QuantitationMethodRepository.findQuantMethodForTypeAndAbundanceUnit(
      udsEM,
      "label_free",  // FIXME: should be parsed from config
      "feature_intensity" // FIXME: should be parsed from config
    )
    
    val quantifier = new Quantifier(
      executionContext = providerContext,
      name = quantifyConfig.datasetName,
      description = quantifyConfig.datasetDescription,
      projectId = projectId,
      methodId = udsQuantMethod.getId,
      experimentalDesign = expDesign,
      quantConfigAsMap = quantConfigAsMap
    )
    quantifier.run()
  }
  
}