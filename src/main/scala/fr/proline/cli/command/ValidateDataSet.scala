package fr.proline.cli.command

import java.io.File

import javax.persistence.EntityManager

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.LazyLogging

import fr.profi.jdbc.easy.EasyDBC
import fr.profi.util.StringUtils
import fr.profi.util.collection._
import fr.proline.cli.DbConnectionHelper
import fr.proline.cli.ProlineCommands
import fr.proline.cli.config.ValidateDataSetConfig
import fr.proline.context.DatabaseConnectionContext
import fr.proline.context.IExecutionContext
import fr.proline.core.dal.DoJDBCReturningWork
import fr.proline.core.dal.context._
import fr.proline.core.dal.tables.SelectQueryBuilder._
import fr.proline.core.dal.tables.SelectQueryBuilder3
import fr.proline.core.dal.tables.msi._
import fr.proline.core.dal.tables.uds.UdsDbDataSetColumns
import fr.proline.core.dal.tables.uds.UdsDbDataSetTable
import fr.proline.core.orm.uds.Aggregation
import fr.proline.core.orm.uds.Aggregation.ChildNature
import fr.proline.core.orm.uds.Dataset
import fr.proline.core.orm.uds.Dataset.DatasetType
import fr.proline.core.orm.uds.IdentificationDataset
import fr.proline.core.orm.uds.Project
import fr.proline.core.service.uds.IdentificationTreeValidator

case class DatasetDescriptor(
  id: Option[Long],
  result_set_id: Option[Long],
  child_ds_type: String,
  name: String,
  biological_sample_name: Option[String] = None,
  biological_group_name: Option[String] = None
)

case class ResultFileDetail(
  id: Long,
  resultFileName: String,
  peaklistPath: Option[String],
  rawFileIdentifier: Option[String]
)

object ValidateDataSet extends LazyLogging {
  
  private val DsTable = UdsDbDataSetTable
  private val DsCols = UdsDbDataSetColumns
  
  private val udsAggregationClass = classOf[fr.proline.core.orm.uds.Aggregation]
  private val udsProjectClass = classOf[fr.proline.core.orm.uds.Project]
  private val udsDatasetClass = classOf[fr.proline.core.orm.uds.Dataset]
  private val udsRawFileClass = classOf[fr.proline.core.orm.uds.RawFile]
  
  /** Get Aggregation for a given ChildNature **/
  private var _aggregationByNature: Map[ChildNature, Aggregation] = null
  def getAggregationByNature()(implicit execCtx: IExecutionContext): Map[ChildNature, Aggregation] = synchronized {
    if (_aggregationByNature != null) return _aggregationByNature
    val jpqlSelect = s"SELECT aggreg FROM ${udsAggregationClass.getName} aggreg"

    _aggregationByNature = execCtx.getUDSDbConnectionContext.getEntityManager.createQuery(jpqlSelect, udsAggregationClass).getResultList().map { aggregation =>
      aggregation.getChildNature -> aggregation
    } toMap
    
    _aggregationByNature
  }
  
  def apply(configFile: String)(implicit execCtx: IExecutionContext) {
    require(execCtx != null, "execCtx is null")

    val fullConfig = ValidateDataSetConfig.loadAndParseConfig(configFile)
    logger.debug("Validation data set config is: " + fullConfig)
    assert(fullConfig.validationConfig.isDefined, "validation config was not provided or not parsed properly")
    
    val projectId = fullConfig.projectId.getOrElse(ProlineCommands.DEFAULT_PROJECT_ID)
    val mergeResultSets = fullConfig.mergeResultSets
    
    val msiDbCtx = execCtx.getMSIDbConnectionContext
    
    val resultSetIds = DoJDBCReturningWork.withEzDBC(msiDbCtx) { msiEzDBC =>
      
      msiEzDBC.inTx { tx =>
        // Update peaklist raw file identifier
        msiEzDBC.executePrepared("UPDATE peaklist SET raw_file_identifier = ? WHERE id = ?") { stmt =>
          msiEzDBC.selectAndProcess("SELECT id, path FROM peaklist") { r =>
            val path = r.getString("path")
            val rawFileIdentifierOpt = new File(path).getName.split("""\.""").headOption
            if (rawFileIdentifierOpt.isDefined) {
              stmt.addString(rawFileIdentifierOpt.get)
              stmt.addLong(r.getLong("id"))
              stmt.execute()
            }
          }
        }
      } // ends msiEzDBC.inTx

      msiEzDBC.selectLongs("SELECT id FROM result_set WHERE type = 'SEARCH'")
    }
    
    val rsDetails = this.getResultSetsFileDetails(
      msiDbCtx,
      resultSetIds
    )
    
    val dsChildren = rsDetails.toArray.map { rsDetail =>
      DatasetDescriptor(
        id = None,
        result_set_id = Some(rsDetail.id),
        child_ds_type = "IDENTIFICATION",
        name = rsDetail.rawFileIdentifier.getOrElse(rsDetail.resultFileName),
        biological_sample_name = None,
        biological_group_name = None
      )
    }

    // FIXME: we have a "database is locked" exception if we don't force beginTransaction
    execCtx.getUDSDbConnectionContext.beginTransaction()

    var datasetId = 0L
    execCtx.getUDSDbConnectionContext.tryInTransaction {
      
      // Create the parent dataset that will hold the merged results
      val newDataset = this.createIdentDatasetTree(
        projectId,
        parentId = None,
        name = fullConfig.datasetName,
        children = dsChildren,
        description = None
      )

      val udsEM = execCtx.getUDSDbConnectionContext.getEntityManager
      udsEM.flush()

      datasetId = newDataset.getId.longValue()
      //logger.debug("New DATASET ID = " + datasetId)

      // Retrieve identification datasets ids
      val udsIdentTreeDS = udsEM.find(classOf[fr.proline.core.orm.uds.Dataset], datasetId)

      // Patch children datasets to fix an ORM bug
      for (childDs <- udsIdentTreeDS.getChildren()) {
        childDs.setChildren(new java.util.ArrayList()) // FIXME: fix proline-orm and remove this WORKAROUND
      }
      
      IdentificationTreeValidator.validateIdentificationTrees( 
        execCtx,
        Array(datasetId),
        mergeResultSets,
        false, //useTdCompet : DEPRECATED 
        fullConfig.validationConfig.get
      )
      
    }
    
  }
  
  /** Create the root dataset and its children (dataset creation) **/
  def createIdentDatasetTree(
    projectId: Long,
    parentId: Option[Long],
    name: String,
    children: Array[DatasetDescriptor],
    description: Option[String]
  )(implicit execCtx: IExecutionContext): Dataset = {

    val udsDbCtx = execCtx.getUDSDbConnectionContext

    var rootDs: Dataset = null

    // Transaction context
    udsDbCtx.tryInTransaction {

      implicit val udsEm = udsDbCtx.getEntityManager

      // Get last dataset number
      val lastDsNumber = DoJDBCReturningWork.withEzDBC(udsDbCtx) { ezDBC =>
        this._getLastDatasetNumber(ezDBC, projectId, parentId)
      }

      // Retrieve project
      val project = this._getProject(projectId)
      val parentDatasetOpt = parentId.map { id =>
        udsEm.find(udsDatasetClass, id)
      }

      // Create parent dataset (tree root)
      rootDs = this._createAndPersistDataset(
        number = lastDsNumber + 1,
        name = name,
        description = description,
        dsType = DatasetType.AGGREGATE,
        project = project,
        parentDataset = parentDatasetOpt
      )

      udsEm.persist(rootDs)
      logger.debug(s"Created root dataset, project ID = $projectId, ID = " + rootDs.getId)

      // Create annotation and/or leaf datasets
      this._createRootChildrenDatasets(project, rootDs, children)(execCtx)
    }

    require(rootDs != null, "createIdentDatasetTree: rootDs should not be null")

    rootDs
      
  } // end of createIdentDatasetTree
  
  /** Get a project in DB **/
  private def _getProject(id: Long)(implicit udsEm: EntityManager): Project = {
    val project = udsEm.find(udsProjectClass, id)
    assert(project != null, "Can't find a project with id=" + id)
    project
  }

  /** Get a dataset in DB **/
  private def _getDataset(id: Long)(implicit udsEm: EntityManager): Dataset = {
    udsEm.find(udsDatasetClass, id)
  }
  
  /** Get the highest number attributed to a dataset in DB **/
  private def _getLastDatasetNumber(ezDBC: EasyDBC, projectId: Long, parentIdOpt: Option[Long]): Int = {
    val parentIdClause = parentIdOpt.map(id => s" = ${id}").getOrElse(" IS NULL ")

    val lastDsNumberSqlQuery = s"SELECT ${DsCols.NUMBER} FROM ${DsTable} " +
      s"WHERE ${DsCols.PARENT_DATASET_ID} $parentIdClause " +
      s"AND project_id = $projectId " + // added by DBO
      s"ORDER BY ${DsCols.NUMBER} DESC LIMIT 1 "

    logger.debug("lastDsNumberSqlQuery: " + lastDsNumberSqlQuery)

    ezDBC.selectHeadOption(lastDsNumberSqlQuery) { r => r.nextInt } getOrElse (0)
  } // end of getLastDatasetNumber

  /** Create the root dataset children at creation **/
  private def _createRootChildrenDatasets(
    project: Project,
    parentDataset: Dataset,
    children: Array[DatasetDescriptor]
  )(implicit execCtx: IExecutionContext) {
    
    val aggregationByNature = getAggregationByNature()

    var bioGroupNumber = 0

    val sortedGroupNames = children.map(_.biological_group_name).distinct
    val childrenByGroupName = children.groupBy(_.biological_group_name)

    sortedGroupNames.foreach { bioGroupNameOpt =>

      // If there are some biological groups
      if (bioGroupNameOpt.isDefined) {
        bioGroupNumber += 1

        // Update root DS aggregation type
        if (parentDataset.getAggregation == null) {
          parentDataset.setAggregation(aggregationByNature.apply(ChildNature.BIOLOGICAL_GROUP))
        }

        // Create biological group as a node
        val bioGroupDS = this._createAndPersistDataset(
          number = bioGroupNumber,
          name = bioGroupNameOpt.get,
          dsType = DatasetType.AGGREGATE,
          project = project,
          parentDataset = Some(parentDataset)
        )(execCtx.getUDSDbConnectionContext.getEntityManager)

        logger.debug(s"Created biological group with name= ${bioGroupDS.getName},ID= ${bioGroupDS.getId}, parent ID= ${parentDataset.getId}")

        // Create biological group children
        val bgChildren = childrenByGroupName(bioGroupNameOpt)
        this._createBioGroupChildrenDatasets(
          project = project,
          parentDataset = bioGroupDS,
          childrenByBioSampleName = bgChildren.groupBy { _.biological_sample_name }
        )
      }
      
      // If there is no biological group, try next children level
      else this._createBioGroupChildrenDatasets(
        project = project,
        parentDataset = parentDataset,
        childrenByBioSampleName = children.groupBy { _.biological_sample_name }
      )
    }
  } // end of _createRootChildrenDatasets

  /** Create the children datasets of a biological group dataset (in new dataset tree creation) **/
  private def _createBioGroupChildrenDatasets(
    project: Project,
    parentDataset: Dataset,
    childrenByBioSampleName: Map[Option[String], Array[DatasetDescriptor]]
  )(implicit execCtx: IExecutionContext) {
    
    val aggregationByNature = getAggregationByNature()
    
    var bioSampleNumber = 0
    implicit val udsEm = execCtx.getUDSDbConnectionContext.getEntityManager

    for ((bioSampleNameOpt, sampleChildren) <- childrenByBioSampleName) {

      // If there are some biological samples
      if (bioSampleNameOpt.isDefined) {
        bioSampleNumber += 1

        // Update parent DS aggregation
        if (parentDataset.getAggregation == null) {
          parentDataset.setAggregation(aggregationByNature(ChildNature.BIOLOGICAL_SAMPLE))
        }

        // Create biological sample dataset (2 cases)

        // If this biological sample is a leaf, create it as an identification dataset
        val isLeaf = sampleChildren.length == 1
        val bioSample: Dataset = if (isLeaf) {

          val dsAsArray = this._createIdentificationDatasets(
            project = project,
            parentDataset = parentDataset,
            leaves = sampleChildren
          )

          dsAsArray.head
        }

        // If it's an aggreagation dataset, create it then its children
        else {
          val bioSampleDS = this._createAndPersistDataset(
            number = bioSampleNumber,
            name = bioSampleNameOpt.get,
            dsType = DatasetType.AGGREGATE,
            project = project,
            parentDataset = Some(parentDataset),
            aggregation = Some(aggregationByNature(ChildNature.SAMPLE_ANALYSIS))
          )

          this._createIdentificationDatasets(
            project,
            parentDataset = bioSampleDS,
            leaves = sampleChildren,
            areLeavesSampleAnalysis = true
          )

          bioSampleDS
        }

        logger.debug(s"Created biological sample with name= ${bioSample.getName},ID= ${bioSample.getId}, parent ID= ${parentDataset.getId} (leaf = $isLeaf)")
        
      } // If there is no biological sample, just create children
      else {
        this._createIdentificationDatasets(
          project,
          parentDataset,
          sampleChildren
        )
      }
    }
  } // end of _createBioGroupChildrenDatasets

  /** Create the identification datasets of a parent dataset (in new dataset tree creation) **/
  private def _createIdentificationDatasets(
    project: Project,
    parentDataset: Dataset,
    leaves: Array[DatasetDescriptor],
    areLeavesSampleAnalysis: Boolean = false
  )(implicit execCtx: IExecutionContext): Array[Dataset] = {
    
    val identDatasets: ArrayBuffer[IdentificationDataset] = new ArrayBuffer(leaves.length)
    implicit val udsEm = execCtx.getUDSDbConnectionContext.getEntityManager
    
    val aggregationByNature = getAggregationByNature()

    var i = 0
    val createdDatasets = leaves.map { leafDsDesc =>
      i += 1

      logger.debug("Creating child dataset, parent ID=" + parentDataset.getId)
      val childType = DatasetType.valueOf(leafDsDesc.child_ds_type)
      val nodeName = leafDsDesc.name

      val ds = childType match {

        // New IDENTIFICATION
        case DatasetType.IDENTIFICATION => {

          // Get result set
          val rsIdOpt = leafDsDesc.result_set_id
          require(rsIdOpt.isDefined, "result_set_id is not defined")
          logger.debug("-> create identification dataset for result set with ID=" + rsIdOpt.get)

          // Create identification dataset
          val newDs = this._createAndPersistDataset(
            number = i,
            name = nodeName,
            dsType = childType,
            resultSetId = rsIdOpt,
            project = project,
            parentDataset = Some(parentDataset)
          )

          // Update parent DS aggregation
          if (parentDataset.getAggregation == null) {
            parentDataset.setAggregation(
              if (areLeavesSampleAnalysis) aggregationByNature(ChildNature.SAMPLE_ANALYSIS)
              else aggregationByNature(ChildNature.BIOLOGICAL_SAMPLE)
            )
          }

          newDs
        }

        // Duplicate datasets of type AGGREGATION / QUANTITATION
        case _ => {
          val initDatasetId = leafDsDesc.id
          require(initDatasetId.isDefined, "initial dataset_id is not defined")
          logger.debug(s"-> duplicate dataset with ID= =" + initDatasetId.get)

          // TODO: throw exceptions instead of returning response
          this._cloneIdentDatatset(
            project,
            initDatasetId.get,
            Some(parentDataset),
            Some(nodeName)
          )
        }
      }

      // Put new dataset in pool to be linked with raw file
      ds match {
        case identDs: IdentificationDataset => if (identDs.getResultSetId != null) identDatasets += identDs
        case _                              => {}
      }

      ds
    }

    // Link identification datasets to their raw files (if rsID is defined)
    _linkDatasetsToRawFiles( identDatasets.toArray)

    createdDatasets
  } // end of _createIdentificationDatasets

  /** Create and persist a dataset **/
  private def _createAndPersistDataset(
    project: Project,
    number: Int,
    name: String,
    dsType: DatasetType,
    aggregation: Option[Aggregation] = None,
    description: Option[String] = None,
    resultSetId: Option[Long] = None,
    resultSummaryId: Option[Long] = None,
    parentDataset: Option[Dataset] = None
  )(implicit udsEm: EntityManager): Dataset = {

    val newDs = dsType match {
      case DatasetType.IDENTIFICATION => {
        val ds = new IdentificationDataset()
        ds.setProject(project)
        ds
      }

      case _ => new Dataset(project)
    }

    newDs.setNumber(number)
    newDs.setName(name)
    newDs.setType(dsType)
    aggregation.map(newDs.setAggregation(_))
    description.map(newDs.setDescription(_))
    resultSetId.map(newDs.setResultSetId(_))
    resultSummaryId.map(newDs.setResultSummaryId(_))

    udsEm.persist(newDs)

    // Add child DS to its parent (auto-increment children count)
    parentDataset.map(_.addChild(newDs))

    // Link to raw file if needed
    //if (dsType == DatasetType.IDENTIFICATION && resultSetId.isDefined) _linkDatasetToRawFiles(execCtx, newDs)

    newDs
  } // end of createAndPersistDataset

  /** Link a dataset to its raw file **/
  /*private def _linkDatasetToRawFiles(
    execCtx: IExecutionContext,
    identDataset: IdentificationDataset
  ) {
    
    val udsEm = execCtx.getUDSDbConnectionContext.getEntityManager

    val resultSetId = identDataset.getResultSetId.longValue()
    val rfDetails = ResultSetLoader.getResultSetsFileDetails(execCtx.getMSIDbConnectionContext, Array(resultSetId)).head
    val rawFile = udsEm.find(udsRawFileClass, rfDetails.rawFileIdentifier)
    identDataset.setRawFile(rawFile)
    // ? setRun
    // ? udsEm.persist(ds)
  }*/
  private def _linkDatasetsToRawFiles(identDatasets: Array[IdentificationDataset])(implicit execCtx: IExecutionContext) {

    val resultSetIds = identDatasets.map(_.getResultSetId.longValue())
    logger.debug("Linking identification datasets to their raw files. Dataset IDs= " + resultSetIds.mkString(", "))

    val rfDetails = this.getResultSetsFileDetails(execCtx.getMSIDbConnectionContext, resultSetIds)
    val rfDetailByRsId = rfDetails.mapByLong(_.id)
    assert(identDatasets.length == rfDetailByRsId.size, "dsIdByRsId and rfDetailByRsId should have the same size")

    val udsEm = execCtx.getUDSDbConnectionContext.getEntityManager

    for (
      ds <- identDatasets;
      rsId = ds.getResultSetId;
      rfDetails = rfDetailByRsId(rsId)
    ) {
      val rawFile = udsEm.find(udsRawFileClass, rfDetails.rawFileIdentifier.get)
      if (rawFile != null) {
        ds.setRawFile(rawFile)
        val runs = rawFile.getRuns.toList
        if (runs.nonEmpty) ds.setRun(runs.head)
      }
    }
  }
  
  /** Duplicate an identification dataset **/
  private def _cloneIdentDatatset(
    project: Project,
    datasetId: Long,
    parentDataset: Option[Dataset] = None,
    newName: Option[String] = None
  )(implicit execCtx: IExecutionContext): Dataset = {

    // Get entity manager
    val udsDbCtx = execCtx.getUDSDbConnectionContext
    val udsEm = udsDbCtx.getEntityManager

    // Get initial dataset
    val initDs = udsEm.find(udsDatasetClass, datasetId)
    assert(initDs != null, "Unexisting dataset can't be duplicated")

    // Create new dataset
    val newDs = initDs.getType match {

      case DatasetType.IDENTIFICATION => {
        val ds = new IdentificationDataset()
        ds.setProject(project)
        ds
      }

      case _ => new Dataset(project)
    } // copied from _createAndPersist

    // Set number
    val lastDsNumber = DoJDBCReturningWork.withEzDBC(udsDbCtx) { ezDBC =>
      this._getLastDatasetNumber(ezDBC, project.getId, None) //parentDataset.map(_.getId))
    }
    newDs.setNumber(lastDsNumber + 1)

    // Set name
    if (newName.isDefined && StringUtils.isNotEmpty(newName.get)) newDs.setName(newName.get)
    else newDs.setName(initDs.getName)

    // Copy other initial dataset properties
    val desc = initDs.getDescription
    if (desc != null) newDs.setDescription(desc)

    val keywords = initDs.getKeywords
    if (keywords != null) newDs.setKeywords(keywords)

    val dsType = initDs.getType
    if (dsType != null) newDs.setType(dsType)

    val fractionation = initDs.getFractionation
    if (fractionation != null) newDs.setFractionation(fractionation)

    val aggregation = initDs.getAggregation
    if (aggregation != null) newDs.setAggregation(aggregation)

    // TODO: remove me?
    val serializedProperties = initDs.getSerializedProperties
    if (serializedProperties != null) newDs.setSerializedProperties(serializedProperties)

    val resultSetId = initDs.getResultSetId
    if (resultSetId != null) newDs.setResultSetId(resultSetId)

    // Don't copy result summary

    // Can't duplicate quantitation node => don't copy quanti-specific fields:
    // sampleAnalyses, biologicalSamples, groupSetups, quantitationChannels,
    // method, masterQuantitationChannels, objectTreeIdByName

    // Persist new dataset
    udsEm.persist(newDs)

    // Add new dataset to its parent
    parentDataset.map(_.addChild(newDs))

    // Duplicate children datasets
    val initDsChildrenIds = initDs.getChildren.map(_.getId)
    val childrenIndentDatasets: ArrayBuffer[IdentificationDataset] = new ArrayBuffer(initDsChildrenIds.length)

    initDsChildrenIds.map { dsChildId =>
      val ds = this._cloneIdentDatatset(project, dsChildId, Some(newDs))
      ds match {
        case identDs: IdentificationDataset => if (identDs.getResultSetId != null) childrenIndentDatasets += identDs
        case _                              => {}
      }
    }

    // Link children to their raw files
    if (childrenIndentDatasets.nonEmpty) _linkDatasetsToRawFiles(childrenIndentDatasets.toArray)

    // Return duplicated dataset
    newDs
  } // end of _cloneIdentDatatset
  
  def getResultSetsFileDetails(msiDbCtx: DatabaseConnectionContext, resultSetIds: Seq[Long]): Seq[ResultFileDetail] = {
    require(resultSetIds != null, "resultSetIds is null")
    if (resultSetIds.isEmpty) return Seq.empty[ResultFileDetail]

    val sqb = new SelectQueryBuilder3(
      MsiDbResultSetTable,
      MsiDbMsiSearchTable,
      MsiDbPeaklistTable
    )

    val sqlQuery = sqb.mkSelectQuery { case tuple =>
      val (t1, t2, t3) = (MsiDbResultSetColumns, MsiDbMsiSearchColumns, MsiDbPeaklistColumns)

      List(t1.ID, t2.RESULT_FILE_NAME, t3.PATH, t3.RAW_FILE_IDENTIFIER) ->
        " WHERE " ~ t1.MSI_SEARCH_ID ~ "=" ~ t2.ID ~
        " AND " ~ t2.PEAKLIST_ID ~ "=" ~ t3.ID ~
        " AND " ~ t1.ID ~ " IN (" ~ resultSetIds.mkString(",") ~ ")"
    }

    DoJDBCReturningWork.withEzDBC(msiDbCtx) { msiEzDBC =>
      msiEzDBC.select(sqlQuery) { r =>
        ResultFileDetail(
          r.nextLong,
          r.nextString,
          r.nextStringOption,
          r.nextStringOption
        )
      }
    }
  }

}