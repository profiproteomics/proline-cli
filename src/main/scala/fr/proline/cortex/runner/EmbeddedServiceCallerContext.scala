package fr.proline.cortex.runner

// FIXME: this file is duplicated in PWX

import com.typesafe.config.Config
import fr.profi.pwx.dal.ProlineDataStoreHandling
import fr.proline.config.MountPointPathConverter
import fr.proline.context._
import fr.proline.core.dal.BuildLazyExecutionContext
import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.repository.IDataStoreConnectorFactory
import fr.proline.repository.IDatabaseConnector
import fr.proline.repository.ProlineDatabaseType

// TODO: define a second EmbeddedServiceCallerContext designed for single threaded services
case class EmbeddedServiceCallerContext(
  val clientName: Option[String],
  val dataStoreConnectorFactory: IDataStoreConnectorFactory,
  val prolineConfig: Config,
  val mountPointPathConverter: MountPointPathConverter
) extends IServiceCallerContext with ProlineDataStoreHandling {
  
  def mountPointRegistry = mountPointPathConverter.mountPointRegistry

  protected val dsConnectorFactory = dataStoreConnectorFactory
  
  // TODO: define a proper ExecutionContext for futures
  val threadExecutionContext = scala.concurrent.ExecutionContext.global
  
  def close() {}

  // TODO: remove me ???
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
          dataStoreConnectorFactory.closeLcMsDbConnector(projectID)
        } else if (dbType == ProlineDatabaseType.MSI) {
          logger.info(s"Closing database connector for MSIdb with project id=$projectID (EntityManagerCount equals zero)")
          dataStoreConnectorFactory.closeMsiDbConnector(projectID)
        }
      }

    }

    BuildLazyExecutionContext(dataStoreConnectorFactory, projectID, useJPA, Some(onConnectionContextClose))
  }
  
  // TODO: update the eProlineDatastoreHandling to tue this signature
  /*def inExecutionCtx[T](projectId: Long)(execCtxFn: IExecutionContext => T ): T = {
    this.inExecutionCtx(projectId, execCtxFn)
  }*/
  
  def inExecutionCtx[T](projectId: Long, useJPA: Boolean)(execCtxFn: IExecutionContext => T ): T = {
    
    // Close database connector if ProlineDatabaseType is LCMS or MSI
    /*val onConnectionContextClose = { dbConnector: IDatabaseConnector =>
      val dbType = dbConnector.getProlineDatabaseType
      if( dbConnector.isClosed == false && (dbType == ProlineDatabaseType.LCMS || dbType == ProlineDatabaseType.MSI) ) {
        dbConnector.close()
      }
    }*/
    
    // Create database context
    val execCtx = BuildLazyExecutionContext(
      dsConnectorFactory,
      projectId,
      useJPA = useJPA,
      onConnectionContextClose = None //Some(onConnectionContextClose)
    )
    
    try {
      // Execute function
      execCtxFn(execCtx)
    } finally {
      // Close execution context
      tryToCloseExecContext( execCtx )
    }
    
  }
  
  // TODO: update the eProlineDatastoreHandling to tue this signature
  /*def inUdsDbCtx[T](inTx: Boolean = false )(udsDbCtxFn: UdsDbConnectionContext => T): T = {
    this.inUdsDbCtx(udsDbCtxFn, inTx)
  }*/

  /*
  // TODO: update the eProlineDatastoreHandling to tue this signature
  def inMsiDbCtx[T](projectId: Long, inTx: Boolean = false)(msiDbCtxFn: MsiDbConnectionContext => T): T = {
    this.inMsiDbCtx(projectId, msiDbCtxFn, inTx)
  }*/
  
/*def getUDSDbConnectionContext(): UdsDbConnectionContext = {
    val udsUdsDbConnector = this.dataStoreConnectorFactory.getUdsDbConnector()
    new UdsDbConnectionContext(udsUdsDbConnector)
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
  }*/

}