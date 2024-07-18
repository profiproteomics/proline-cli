package fr.proline.cli

import com.beust.jcommander._
import com.typesafe.scalalogging.LazyLogging
import fr.profi.util.ThreadLogger
import fr.proline.cortex.CortexCLI

/**
 * @auhor David Bouyssie
 */
object ProlineCLI extends LazyLogging { //App with

  /*// Disable hibernate logs
  org.slf4j.LoggerFactory
    .getLogger("org.hibernate")
    .asInstanceOf[ch.qos.logback.classic.Logger]
    .setLevel(ch.qos.logback.classic.Level.ERROR)

  // Disable c3p0 logs
  org.slf4j.LoggerFactory
    .getLogger("com.mchange.v2")
    .asInstanceOf[ch.qos.logback.classic.Logger]
    .setLevel(ch.qos.logback.classic.Level.ERROR)*/
  
  // TODO: put in profi-scala-commons (shared with proline admin and mzdb-processing)
  trait JCommandReflection {
    lazy private val _parametersAnnotation = this.getClass().getAnnotation(classOf[Parameters])

    object Parameters {
      lazy val names = _parametersAnnotation.commandNames()
      lazy val firstName = names.head
      lazy val description = _parametersAnnotation.commandDescription()
    }
  }
  
  @Parameters(commandNames = Array("import_result_files"), commandDescription = "Import result file in the Proline Databases", separators = "=")
  private[cli] object ImportResultFilesCommand extends JCommandReflection {

    @Parameter(names = Array("-i", "--input_file_list"), description = "Text file containing the list of input files to import", required = true)
    var inputFileList: String = ""

    @Parameter(names = Array("-c", "--config_file"), description = "Path to the import configuration file", required = false)
    var configFile: String = _
  }
  
  @Parameters(commandNames = Array("validate_dataset"), commandDescription = "Merge and validate imported result files", separators = "=")
  private[cli] object ValidateDataSetCommand extends JCommandReflection {
    @Parameter(names = Array("-c", "--config_file"), description = "Path to the validation configuration file", required = false)
    var configFile: String = ""
  }
  
  @Parameters(commandNames = Array("quantify_dataset"), commandDescription = "Quantify dataset using label-free method", separators = "=")
  private[cli] object QuantifyDataSetCommand extends JCommandReflection {

    @Parameter(names = Array("-ed", "--exp_design_file"), description = "Path to the experimental design file", required = true)
    var expDesignFile: String = ""
    
    @Parameter(names = Array("-c", "--config_file"), description = "Path to the quant configuration file", required = true)
    var configFile: String = ""
  }
  
  @Parameters(commandNames = Array("run_lfq_workflow"), commandDescription = "Run the full Proline label-free workflow", separators = "=")
  private[cli] object RunLfqWorkflowCommand extends JCommandReflection {

    @Parameter(names = Array("-i", "--ident_file_list"), description = "Text file containing the list of identification files to import", required = true)
    var identFileList: String = ""
    
    @Parameter(names = Array("-ed", "--exp_design_file"), description = "Path to the experimental design file", required = true)
    var expDesignFile: String = ""
    
    @Parameter(names = Array("-c", "--config_file"), description = "Path to the quant configuration file", required = true)
    var configFile: String = ""
  }

  def main(args: Array[String]): Unit = {
    
    // Customize the ThreadLogger for uncaught exceptions
    Thread.currentThread.setUncaughtExceptionHandler(new ThreadLogger(logger.underlying.getName()))
    
    // Instantiate a JCommander object and set some commands
    val jCmd = new JCommander()
    jCmd.addCommand(ImportResultFilesCommand)
    jCmd.addCommand(ValidateDataSetCommand)
    jCmd.addCommand(QuantifyDataSetCommand)
    jCmd.addCommand(RunLfqWorkflowCommand)
    jCmd.addCommand(CortexCLI.RunCortexServiceCommand)

    // Try to parse the command line
    var parsedCommand = ""
    try {
      jCmd.parse(args: _*)
      parsedCommand = jCmd.getParsedCommand()

      if (parsedCommand == null || parsedCommand.isEmpty()) {
        logger.error("A command must be provided!")
        jCmd.usage()
        System.exit(1)
      } else {
        logger.info(s"Running '$parsedCommand' command...")
      }
      
      import ProlineCommands._

      if (parsedCommand != CortexCLI.RunCortexServiceCommand.Parameters.firstName) {
        // Initialize Proline command runner
        ProlineCommands.init()
      }

      // Execute parsed command
      parsedCommand match {
        case ImportResultFilesCommand.Parameters.firstName => {
          importResultFiles(
            ImportResultFilesCommand.inputFileList,
            Option(ImportResultFilesCommand.configFile)
          )
        }
        case ValidateDataSetCommand.Parameters.firstName => {
          mergeAndValidateDataSet(
            ValidateDataSetCommand.configFile
          )
        }
        case QuantifyDataSetCommand.Parameters.firstName => {
          quantifyDataSet(
            QuantifyDataSetCommand.expDesignFile,
            QuantifyDataSetCommand.configFile
          )
        }
        case RunLfqWorkflowCommand.Parameters.firstName => {
          runLabelFreeWorkflow(
            RunLfqWorkflowCommand.identFileList,
            RunLfqWorkflowCommand.configFile,
            RunLfqWorkflowCommand.configFile,
            RunLfqWorkflowCommand.expDesignFile,
            RunLfqWorkflowCommand.configFile
          )
        }
        case _ => {
          fr.proline.cortex.CortexCLI.runCommand(args)
          //throw new MissingCommandException(s"Unknown command '${jCmd.getParsedCommand}'")
        }
      }
      
    } catch {

      case pEx: ParameterException => {
        println()
        logger.warn("Invalid command or parameter", pEx)
        jCmd.usage()
        System.exit(1)
      }

      case t: Throwable => {
        println()
        logger.error(s"Execution of command '$parsedCommand' failed", t)
        System.exit(1)
      }
      
    } finally {
      // Tear down Proline command runner
      ProlineCommands.tearDown()
    }
    
  }


}