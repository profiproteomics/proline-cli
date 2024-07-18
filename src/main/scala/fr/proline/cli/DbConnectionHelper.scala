package fr.proline.cli

import com.typesafe.scalalogging.LazyLogging

import fr.proline.admin.service.db.SetupProline
import fr.proline.context.DatabaseConnectionContext
import fr.proline.context.IExecutionContext
import fr.proline.core.dal.BuildLazyExecutionContext
import fr.proline.core.orm.util.DataStoreConnectorFactory
import fr.proline.repository.IDataStoreConnectorFactory
import fr.proline.repository.IDatabaseConnector
import fr.proline.repository.ProlineDatabaseType

object DbConnectionHelper extends LazyLogging {

  private var m_dsConnectorFactory: IDataStoreConnectorFactory = null

  def initDataStore() {
    m_dsConnectorFactory = DataStoreConnectorFactory.getInstance()

    if (!m_dsConnectorFactory.isInitialized) {
      val prolineConfig = SetupProline.config

      val udsDbProperties = prolineConfig.udsDBConfig.dbConnProperties

      val connProps = udsDbProperties.asInstanceOf[java.util.HashMap[String,String]]

      // Hack H2 connection properties to allow connections from multiple processes
      import fr.proline.repository.AbstractDatabaseConnector.{PERSISTENCE_JDBC_DRIVER_KEY, PERSISTENCE_JDBC_URL_KEY}
      if (connProps.get(PERSISTENCE_JDBC_DRIVER_KEY) == "org.h2.Driver") {
        val patchedURL = connProps.get(PERSISTENCE_JDBC_URL_KEY) + ";AUTO_SERVER=TRUE"
        connProps.put(PERSISTENCE_JDBC_URL_KEY, patchedURL)
      }

      logger.debug("Initializing DataStoreConnectorFactory from UDSdb Properties")

      m_dsConnectorFactory.asInstanceOf[DataStoreConnectorFactory].initialize(
        connProps.asInstanceOf[java.util.HashMap[AnyRef,AnyRef]],
      "Proline CLI"
      )
    }
  }
  
  def getDataStoreConnectorFactory(): IDataStoreConnectorFactory = {
    if (m_dsConnectorFactory == null) {
      initDataStore()
    }
    m_dsConnectorFactory
  }

  def createJPAExecutionContext(projectID: Long): IExecutionContext = {
    createExecutionContext(projectID, true)
  }

  def createSQLExecutionContext(projectID: Long): IExecutionContext = {
    createExecutionContext(projectID, false)
  }

  private def createExecutionContext(projectID: Long, useJPA: Boolean): IExecutionContext = {
    val onConnectionContextClose = { dbConnector: IDatabaseConnector =>
      
      logger.info("onConnectionContextClose called ... ")
      
      if ((dbConnector.getOpenConnectionCount == 0) && (dbConnector.getOpenEntityManagerCount == 0)) {
        
        val dbType = dbConnector.getProlineDatabaseType
        logger.info("Proline database type = " + dbType)
        
        if (dbType == ProlineDatabaseType.LCMS) {
          logger.info(s"Closing database connector for LCMSdb with project id=$projectID (EntityManagerCount equals zero)")
          m_dsConnectorFactory.closeLcMsDbConnector(projectID)
        } else if (dbType == ProlineDatabaseType.MSI) {
          logger.info(s"Closing database connector for MSIdb with project id=$projectID (EntityManagerCount equals zero)")
          m_dsConnectorFactory.closeMsiDbConnector(projectID)
        }
      }

    }

    BuildLazyExecutionContext(DbConnectionHelper.getDataStoreConnectorFactory, projectID, useJPA, Some(onConnectionContextClose))
  }

  // Some reusable try/catch blocks
  def tryToRollbackDbTransaction(dbCtx: DatabaseConnectionContext) {
    if (dbCtx != null) {
      val dbType = dbCtx.getProlineDatabaseType()
      logger.info(s"Rollbacking $dbType DB transaction")

      try {
        dbCtx.rollbackTransaction()
      } catch {
        case ex: Exception => logger.error(s"Error while rollbacking $dbType DB transaction", ex)
      }

    }
  }

  def tryToCloseDbContext(dbCtx: DatabaseConnectionContext) {
    if (dbCtx != null) {
      val dbType = dbCtx.getProlineDatabaseType()
      logger.debug(s"Closing $dbType DB SQL context")

      try {
        dbCtx.close()
      } catch {
        case exClose: Exception => logger.error(s"Error while closing $dbType DB SQL context", exClose)
      }
    }
  }

  def tryToCloseExecContext(execCtx: IExecutionContext) {
    if (execCtx != null) {
      logger.debug("Closing current ExecutionContext")

      try {
        execCtx.closeAll()
      } catch {
        case exClose: Exception => logger.error("Error while closing ExecutionContext", exClose)
      }
    }
  }

  def tryToCloseDataStoreConnectorFactory() {
    if (m_dsConnectorFactory != null) {
      logger.debug("Closing Proline DataStoreConnectorFactory")

      try {
        m_dsConnectorFactory.closeAll()
      } catch {
        case t: Throwable => logger.error("Error while closing DataStoreConnectorFactory", t)
      }
    }
  }
  
}