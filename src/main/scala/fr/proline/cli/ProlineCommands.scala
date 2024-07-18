package fr.proline.cli

import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

import com.beust.jcommander.{ JCommander, MissingCommandException, Parameter, ParameterException, Parameters }
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils

import fr.profi.jdbc.SQLiteSQLDialect
import fr.profi.jdbc.TxIsolationLevels
import fr.profi.jdbc.easy._
import fr.profi.util.ThreadLogger
import fr.proline.admin.service.db.SetupProline
import fr.proline.cli.command._
import fr.proline.cli.config._
import fr.proline.context.IExecutionContext
import fr.proline.core.dal.context._
import fr.proline.core.om.model.msq._
import fr.proline.repository.DriverType

/**
 * @author David Bouyssie
 *
 */
// TODO: rename ProlineCommandRunner
object ProlineCommands extends LazyLogging {
  
  lazy val PROLINE_DATA_DIR_KEY = "proline-config.data-directory"
  lazy val DB_TEMPLATE_DIR = "/extra/h2_templates"
  lazy val DEFAULT_PROJECT_ID = 1L
  lazy val DEFAULT_INSTRUMENT_ID = 4L
  lazy val DEFAULT_PEAKLIST_SOFTWARE_ID = 1L // Extract MSn
  //private val DEFAULT_PEAKLIST_SOFTWARE_ID = 7L // Proline
  //private val DEFAULT_PEAKLIST_SOFTWARE_ID = 12L // ProteoWizard 3.0

  //runCommand()
  
  /*
  /** create project command */
  @Parameters(commandNames = Array("run_lfq_workflow"), commandDescription = "Run label-free workflow", separators = "=")
  private object RunLfqWorkflow extends JCommandReflection {

    @Parameter(names = Array("--ident_files", "-ids"), description = "The list of search results files", required = true)
    var identFileList: String = ""

    @Parameter(names = Array("--validation_config", "-vcfg"), description = "The validation config file", required = true)
    var validationConfigFile: String = ""

    @Parameter(names = Array("--exp_design", "-ed"), description = "The experimental design file", required = true)
    var expDesignFile: String = ""
        
    @Parameter(names = Array("--quant_config", "-qcfg"), description = "The quantitation config file", required = true)
    var quantConfigFile: String = ""
  }
  
  override def main(args: Array[String]): Unit = {
    Thread.currentThread.setUncaughtExceptionHandler(new ThreadLogger(logger.underlying.getName()))

    // Instantiate a JCommander object and affect some commands
    val jCmd = new JCommander()
    jCmd.addCommand(RunLfqWorkflow)

    // Try to parse the command line
    var parsedCommand = ""
    try {
      jCmd.parse(args: _*)

      parsedCommand = jCmd.getParsedCommand()
      println(s"Running '$parsedCommand' command...")

      // Execute parsed command
      parsedCommand match {
        case RunLfqWorkflow.Parameters.firstName => {

          import fr.proline.admin.service.user.CreateProject

          /*val projectId = CreateProject(
            CreateProjectCommand.projectName,
            CreateProjectCommand.projectDescription,
            CreateProjectCommand.ownerId
            
          )*/
          
          runLabelFreeWorkflow(
            "./target/config/import_file_list.txt",
            "./target/config/lfq_workflow.conf",
            "./target/config/quant_exp_design.txt",
            "./target/config/lfq_workflow.conf"
          )

          this.logger.info(s"Label-free workflow successfully executed!")

        }
        case _ => {
          throw new MissingCommandException(s"unknown command '${jCmd.getParsedCommand()}'")
        }
      }

    } catch {

      case pEx: ParameterException => {
        println()
        logger.warn("Invalid command or parameter", pEx)
        jCmd.usage()
      }

      case ex: Exception => {
        println()
        logger.error("Execution of command '" + parsedCommand + "' failed", ex)
      }

    } finally {
      //if (hasDsConnectorFactory) dsConnectorFactory.closeAll()
    }

  }*/
  
  /*private def runCommand(cmd: => Unit) {
    try {
      // Initialize the command runner
      init()
      
      cmd
      
      /*quantifyDataSet(
        "./target/test-classes/quant_exp_design_short.txt",
        "./target/test-classes/quantify_dataset_hocon.conf"
      )*/
      
      runLabelFreeWorkflow(
        "./target/config/import_file_list.txt",
        "./target/config/lfq_workflow.conf",
        "./target/config/lfq_workflow.conf",
        "./target/config/quant_exp_design.txt",
        "./target/config/lfq_workflow.conf"
      )
      
    } catch {
      case t: Throwable => {
        logger.error("Execution of current command failed",t)
      }
    } finally {
      // Tear down Proline command runner
      ProlineCommands.tearDown()
    }
  }*/

  def init() {
    
    println("isRunningFromJar : "+ isRunningFromJar)
    
    val appConfig = ConfigFactory.load(this.getClass.getClassLoader,"application")

    // Retrieve data directory property
    val dataDirectory = if (!appConfig.hasPath(PROLINE_DATA_DIR_KEY)) "./proline_results"
    else {
      appConfig.getString(PROLINE_DATA_DIR_KEY).replaceAll("""\\""", "/")
    }
    
    // Create data directory if not exists
    val dataDirFile = new File(dataDirectory)
    if (!dataDirFile.isDirectory) {
      logger.info(s"Creating database directory: $dataDirFile")
      dataDirFile.mkdirs()
    } else {
      logger.info(s"Database directory already exists at: $dataDirFile")
    }
    
    // Update application config with directory
    val updatedAppConfig = appConfig.withValue(
      PROLINE_DATA_DIR_KEY,
      ConfigValueFactory.fromAnyRef(dataDirectory)
    )
    
    // Replace Configuration in SetupProline singleton by main application.conf file
    SetupProline.setConfigParams(updatedAppConfig)
    
    /*if (isRunningFromJar) {
      new FileInputStream(new File("."+DB_TEMPLATE_DIR))
    } else {
      this.getClass.getResourceAsStream(DB_TEMPLATE_DIR)
    }*/

    println(dataDirFile.list().toList)

    if (!dataDirFile.list().isEmpty) {
      logger.info(s"Database directory already contains files, skipping Proline initialization...")
    } else {

      var isUdsDbCopied = false

      // Copy database templates into the output directory
      if (isRunningFromJar) {
        val dbDir = new File(jarFile.getParentFile,DB_TEMPLATE_DIR)
        for (file <- dbDir.listFiles()) {
          val fileName = file.getName
          val destFile = new File(dataDirectory+"/"+file.getName)

          if (!destFile.isFile) {
            logger.debug(s"Copying file $file to $destFile...")
            Files.copy(file.toPath(), destFile.toPath)
            if (fileName.contains(SetupProline.config.udsDBConfig.dbName)) {
              isUdsDbCopied = true
            }
          } else {
            logger.warn(s"Destination file $destFile already exists and won't be overwritten!")
          }
        }
      }
      else {
        val dbTemplatesDirAsStream = this.getClass.getResourceAsStream(DB_TEMPLATE_DIR)

        try {
          val fileIter = IOUtils.readLines(dbTemplatesDirAsStream).iterator()
          while (fileIter.hasNext) {
            val fileName = fileIter.next().toString
            val isCopied = copyResourceFile(DB_TEMPLATE_DIR+"/"+fileName, dataDirectory)
            if (fileName.contains(SetupProline.config.udsDBConfig.dbName)) {
              isUdsDbCopied = isCopied
            }
          }
        } catch {
          case t: Throwable => {
            logger.error("Error while copying resource",t)
            throw t
          }
        } finally {
          //println("isUdsDbCopied",isUdsDbCopied)
          //println("dbTemplatesDirAsStream equals null", dbTemplatesDirAsStream == null)
          if (dbTemplatesDirAsStream!= null) dbTemplatesDirAsStream.close()
        }
      }

      val driverType = SetupProline.config.udsDBConfig.driverType
      //require(driverType == DriverType.SQLITE,s"Unsupported database backend '$driverType'")

      if (isUdsDbCopied) {
        val dbName = SetupProline.config.udsDBConfig.dbName

        if (driverType == DriverType.SQLITE) {
          Class.forName("org.sqlite.JDBC")
          val ezDBC = EasyDBC(
            DriverManager.getConnection(s"jdbc:sqlite:$dataDirectory/$dbName"),
            SQLiteSQLDialect,
            TxIsolationLevels.SERIALIZABLE
          )
          ezDBC.execute(s"UPDATE external_db SET name = '$dataDirectory/' || name")

          ezDBC.connection.close()
        } else if (driverType == DriverType.H2) {
          Class.forName("org.h2.Driver")

          val ezDBC = EasyDBC(
            DriverManager.getConnection(s"jdbc:h2:$dataDirectory/$dbName", "proline_db_user", "proline")
          )

          // TODO: insert the appropriate HASH value
          ezDBC.execute(s"INSERT INTO user_account(login,password_hash,creation_mode) VALUES ('user1','test','AUTO')")
          //fr.proline.admin.service.user.CreateProject("default project","default project description",1L)

          //ezDBC.execute(s"UPDATE external_db SET name = '$dataDirectory/pdi-db' WHERE name LIKE '%pdi-db%'")
          //ezDBC.execute(s"UPDATE external_db SET name = '$dataDirectory/ps-db' WHERE name LIKE '%ps-db%'")
          ezDBC.execute(s"UPDATE external_db SET name = '$dataDirectory/lcms-db' WHERE name LIKE '%lcms-db%'")
          ezDBC.execute(s"UPDATE external_db SET name = '$dataDirectory/msi-db' WHERE name LIKE '%msi-db%'")

          ezDBC.select("SELECT name FROM external_db") { r =>
            println( r.nextString )
          }

          ezDBC.connection.close()
        } else {
          throw new Exception(s"Unsupported database backend '$driverType'")
        }
      } // ends if (isUdsDbCopied)
    }

    
  }

  def tearDown() {
    logger.info("Tearing down Proline CLI Command runner...")
    
    // Close the Proline datastore
    DbConnectionHelper.tryToCloseDataStoreConnectorFactory()
  }
  
  // TODO: backport file.exists FIX in scala commons
  def pathToStreamOrResourceToStream( path: String, resClass: Class[_] ): InputStream = {
    val file = new File(path)
    
    if (file.exists()) new FileInputStream(file.getAbsolutePath)
    else resClass.getResourceAsStream(path)
  }

  def copyResourceFile(resourceName: String, outputDir: String): Boolean = {
    val fileUrl = getClass().getResource(resourceName)
    //println("fileUrl",fileUrl)

    val destFile = new File(outputDir + "/"  + new File(resourceName).getName)
    if (destFile.exists()) {
      logger.warn(s"Destination file $destFile already exists and won't be overwritten!")
      false
    } else {
      logger.debug(s"Copying file $fileUrl to $destFile...")
      FileUtils.copyURLToFile(fileUrl, destFile)
      true
    }
  }
  
  def runLabelFreeWorkflow(
    identFileList: String,
    identConfigFile: String,
    validationConfigFile: String,
    expDesignFile: String,
    quantConfigFile: String
  ) {
    
    importResultFiles(identFileList, Some(identConfigFile))
    
    mergeAndValidateDataSet(validationConfigFile)

    quantifyDataSet(expDesignFile, quantConfigFile)
    
    /*importResultFiles(
      "./target/test-classes/import_file_list_marc.txt",
      Some("./target/test-classes/import_result_files.conf")
    )
    
    mergeAndValidateDataSet(
      "./target/test-classes/validate_dataset_marc.conf"
    )
    
    quantifyDataSet(
      "./target/test-classes/quant_exp_design_marc.txt",
      "./target/test-classes/quantify_dataset_hocon.conf"
    )*/
    
    exportDataSet()
  }
  
  def importResultFiles(inputFileList: String, configFileOpt: Option[String]) {
    command.ImportResultFiles.apply(inputFileList, configFileOpt)
  }
  
  def mergeAndValidateDataSet(configFile: String) {
    
    implicit val execCtx = DbConnectionHelper.createJPAExecutionContext(DEFAULT_PROJECT_ID)
      
    try {
      ValidateDataSet(configFile)
    } finally {
      DbConnectionHelper.tryToCloseExecContext(execCtx)
    }
  }
  
  def quantifyDataSet(expDesignFile: String, configFile: String) {
    
    implicit val execCtx = DbConnectionHelper.createJPAExecutionContext(DEFAULT_PROJECT_ID)
    
    try {
      QuantifyDataSet(expDesignFile, configFile)
    } finally {
      DbConnectionHelper.tryToCloseExecContext(execCtx)
    }
  }

  def exportDataSet() {
    
    implicit val execCtx = DbConnectionHelper.createJPAExecutionContext(DEFAULT_PROJECT_ID)
    
    try {
      ExportDataSet(SetupProline.config.dataDirectoryOpt.get.toString)
    } finally {
      DbConnectionHelper.tryToCloseExecContext(execCtx)
    }
  }

  lazy val jarFile: File = {
    new File(
      this.getClass.getProtectionDomain()
        .getCodeSource()
        .getLocation()
        .getPath()
    )
  }
  
  lazy val isRunningFromJar: Boolean = {
    jarFile.getName.endsWith(".jar")
  }

  /*private def exportResource(resourceName: String): String = {
    var stream: InputStream = null
    var resStreamOut: OutputStream = null
    var jarFolder: String = null
    try {
      //note that each / is a directory down in the "jar tree" been the jar the root of the tree
      stream = this.getClass.getResourceAsStream(resourceName)
      require(stream != null, s"Cannot get resource '$resourceName' from JAR file.")

      var readBytes: Int = 0
      val buffer = new Array[Byte](4096)
      jarFolder = new File(this.getClass.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getParentFile().getPath().replace('\\', '/');
      resStreamOut = new FileOutputStream(jarFolder + resourceName)
      while ((readBytes = stream.read(buffer)) > 0) {
        resStreamOut.write(buffer, 0, readBytes)
      }
    } finally {
      stream.close()
      resStreamOut.close()
    }

    return jarFolder + resourceName;
  }*/
}

/*
trait JCommandReflection {
  lazy private val _parametersAnnotation = this.getClass().getAnnotation(classOf[Parameters])

  object Parameters {
    lazy val names = _parametersAnnotation.commandNames()
    lazy val firstName = names(0)
    lazy val description = _parametersAnnotation.commandDescription()
  }
}*/