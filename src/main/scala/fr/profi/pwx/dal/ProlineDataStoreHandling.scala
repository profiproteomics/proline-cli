package fr.profi.pwx.dal

import javax.persistence.EntityManager

import com.typesafe.scalalogging.StrictLogging

import fr.proline.context._
import fr.proline.core.dal._
import fr.proline.repository._
import fr.profi.jdbc.easy.EasyDBC

trait ProlineDataStoreHandling extends StrictLogging {

  protected def dsConnectorFactory: IDataStoreConnectorFactory

  private def inDbCtx[D <: DatabaseConnectionContext,T](
    dbCtx: D,
    dbCtxFn: D => T,
    inTx: Boolean = false,
    closeDbConnector: Boolean = false
  ): T = {

    // Create database context
    //val dbCtx = new DatabaseConnectionContext(dbConnector)
    
    val isLocalTx = !dbCtx.isInTransaction()
    var isTxCommitted = false
    var result: T = null.asInstanceOf[T]

    try {

      if (inTx) {
        if (isLocalTx) dbCtx.beginTransaction()

        // Execute function
        result = dbCtxFn(dbCtx)

        if (isLocalTx) dbCtx.commitTransaction()

        isTxCommitted = true
      }
      else {
        // Execute function
        result = dbCtxFn(dbCtx)
      }

    } catch {
      case t: Throwable => {
        logger.error("Error during database transaction: " + t.getMessage)
        throw t
      }
    } finally {
      
      // Rollback transaction
      if( inTx && isLocalTx && isTxCommitted == false ) {
        tryToRollbackDbTransaction(dbCtx)
      }
      
      // Close database context
      if( dbCtx.isClosed == false ) tryToCloseDbContext(dbCtx)
      
      //if(closeDbConnector && dbConnector.isClosed == false) tryToCloseDbConnector(dbConnector)
    }

    result
  }
  
  def inExecutionCtx[T]( projectId: Long, execCtxFn: IExecutionContext => T ): T = {
    
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
      useJPA = true,
      onConnectionContextClose = None //Some(onConnectionContextClose)
    )
    
    // Force initialization of MSIDbConnector when H2 is used (to force injection of AUTO_SERVER=TRUE, which is done bellow)
    if (dsConnectorFactory.getUdsDbConnector.getDriverType == DriverType.H2) {
      this.getMsiDbConnector(projectId)
    }
    
    try {
      // Execute function
      execCtxFn(execCtx)
    } finally {
      // Close execution context
      tryToCloseExecContext( execCtx )
    }
    
  }
  
  def inLcMsDbCtx[T]( projectId: Long, lcmsDbCtxFn: LcMsDbConnectionContext => T, inTx: Boolean = false ): T = {
    val dbCtx = new LcMsDbConnectionContext(dsConnectorFactory.getLcMsDbConnector(projectId))
    this.inDbCtx[LcMsDbConnectionContext,T](dbCtx, lcmsDbCtxFn, inTx, closeDbConnector = true)
  }
  
  protected def getMsiDbConnector(projectId: Long): IDatabaseConnector = {
    val msiDbConnector = dsConnectorFactory.getMsiDbConnector(projectId)
    
    // Enable AUTO_SERVER mode for MSIdb when H2 is used (allows concurrent to access the MSIdb from multiple processes) 
    if (msiDbConnector.getDriverType == DriverType.H2) {
      // FIXME: we should not overwrite existing additional properties => we need a getter in AbstractDatabaseConnector
      val jdbcUrl = msiDbConnector.getProperty(AbstractDatabaseConnector.PERSISTENCE_JDBC_URL_KEY).asInstanceOf[String]
      assert(jdbcUrl != null, AbstractDatabaseConnector.PERSISTENCE_JDBC_URL_KEY + " should be defined")
      
      //println("jdbcUrl: "+ jdbcUrl)
      
      if (!jdbcUrl.contains("AUTO_SERVER")) {
        val extraProps = new java.util.HashMap[AnyRef,AnyRef]()
        extraProps.put(AbstractDatabaseConnector.PERSISTENCE_JDBC_URL_KEY, jdbcUrl+";AUTO_SERVER=TRUE")
        msiDbConnector.setAdditionalProperties(extraProps)
      }
    }
  
    msiDbConnector
  }
  
  def inMsiDbCtx[T]( projectId: Long, msiDbCtxFn: MsiDbConnectionContext => T, inTx: Boolean = false ): T = {
    val dbCtx = new MsiDbConnectionContext(this.getMsiDbConnector(projectId))
    this.inDbCtx[MsiDbConnectionContext,T](dbCtx, msiDbCtxFn, inTx, closeDbConnector = true)
  }
  
  def inUdsDbCtx[T]( udsDbCtxFn: UdsDbConnectionContext => T, inTx: Boolean = false ): T = {
    val dbCtx = new UdsDbConnectionContext(dsConnectorFactory.getUdsDbConnector)
    this.inDbCtx[UdsDbConnectionContext,T](dbCtx, udsDbCtxFn, inTx)
  }
  
  def withLcMsEM[T]( projectId: Long, emWork: EntityManager => T, inTx: Boolean = false ): T = {
    this.inLcMsDbCtx( projectId, { lcmsDbCtx =>
      val res = emWork( lcmsDbCtx.getEntityManager )
      res
    }, inTx = inTx )
  }

  def withLcMsEzDBC[T](projectId: Long, jdbcWork: EasyDBC => T, inTx: Boolean = false): T = {
    this.inLcMsDbCtx(projectId, { lcmsDbCtx =>
      DoJDBCReturningWork.withEzDBC[T](lcmsDbCtx) { ezDBC =>
        jdbcWork(ezDBC)
      }
    }, inTx = inTx)
  }

  def withMsiEM[T]( projectId: Long, emWork: EntityManager => T, inTx: Boolean = false ): T = {
    this.inMsiDbCtx( projectId, { msiDbCtx =>
      val res = emWork( msiDbCtx.getEntityManager )
      res
    }, inTx = inTx )
  }

  def withMsiEzDBC[T]( projectId: Long, jdbcWork: EasyDBC => T, inTx: Boolean = false ): T = {
    this.inMsiDbCtx(projectId, { msiDbCtx =>

      DoJDBCReturningWork.withEzDBC[T](msiDbCtx) { ezDBC =>
        jdbcWork(ezDBC)
      }
    }, inTx = inTx)
  }
  
  def withUdsEM[T]( emWork: EntityManager => T, inTx: Boolean = false ): T = {
    this.inUdsDbCtx( { udsDbCtx =>
      emWork( udsDbCtx.getEntityManager )
    }, inTx = inTx )
  }

  def withUdsEzDBC[T](jdbcWork: EasyDBC => T, inTx: Boolean = false): T = {
    this.inUdsDbCtx({ udsDbCtx =>
      DoJDBCReturningWork.withEzDBC[T](udsDbCtx) { ezDBC =>
        jdbcWork(ezDBC)
      }
    }, inTx = inTx)
  }
  
  def tryToRollbackDbTransaction(dbCtx: DatabaseConnectionContext) {
    if ( dbCtx != null ) {
      val dbType = dbCtx.getProlineDatabaseType()
      logger.warn(s"Rollbacking $dbType DB transaction")

      try {
        dbCtx.rollbackTransaction()
      } catch {
        case ex: Exception => logger.error(s"Error rollbacking $dbType Db Transaction", ex)
      } finally {
        if( dbCtx.isClosed == false ) tryToCloseDbContext(dbCtx)
      }

    }
  }
  
  def tryToCloseDbConnector(dbConnector: IDatabaseConnector) {
    if (dbConnector != null) {
      val dbType = dbConnector.getProlineDatabaseType()
      logger.trace("Closing "+dbType+"db connector")
      
      try {
        dbConnector.close()
      } catch {
        case exClose: Exception => logger.error("Error closing "+dbType+"db connector", exClose)
      }
    }
  }
  
  def tryToCloseDbContext(dbCtx: DatabaseConnectionContext) {
    if (dbCtx != null) {
      val dbType = dbCtx.getProlineDatabaseType()
      logger.trace("Closing "+dbType+"db connection context")
      
      try {
        dbCtx.close()
      } catch {
        case exClose: Exception => logger.error("Error closing "+dbType+"db connection context", exClose)
      }
    }
  }
  
  def tryToCloseExecContext(execCtx: IExecutionContext) {
    if (execCtx != null) {  
      logger.trace("Closing current ExecutionContext")

      try {
        execCtx.closeAll()
      } catch {
        case exClose: Exception => logger.error("Error closing current ExecutionContext", exClose)
      }
    }
  }

}