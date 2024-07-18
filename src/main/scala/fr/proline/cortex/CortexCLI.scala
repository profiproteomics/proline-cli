package fr.proline.cortex

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.beust.jcommander._
import com.thetransactioncompany.jsonrpc2.JSONRPC2Error
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import fr.profi.util.exception.SerializableStackTraceElement
import fr.profi.util.jsonrpc.{BuildJSONRPC2Response, ProfiJSONRPC2Response}
import fr.profi.util.serialization.ProfiJson
import fr.proline.admin.service.db.SetupProline
import fr.proline.cli.DbConnectionHelper
import fr.proline.config.MountPointPathConverter
import fr.proline.cortex.caller.IServiceCall
import fr.proline.cortex.caller.service.dps.msi._
import fr.proline.cortex.caller.service.dps.msq._
import fr.proline.cortex.caller.service.dps.uds._
import fr.proline.cortex.caller.service.seqdb._
import fr.proline.jms.util.JMSConstants.SERVICE_ERROR_CODE


/**
 * @auhor David Bouyssie
 */
object CortexCLI extends LazyLogging { //App with

  // Disable hibernate logs
  /*org.slf4j.LoggerFactory
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
  
  @Parameters(commandNames = Array("run_cortex_service","run-cortex-service"), commandDescription = "Generic command to execute a given cortex service.", separators = "=")
  object RunCortexServiceCommand extends JCommandReflection {

    @Parameter(names = Array("-n", "--name"), description = "The service name", required = true)
    var serviceName: String = _

    @Parameter(names = Array("-c", "--config"), description = "The service config file (JSON content)", required = true)
    var serviceConfig: String = _
  }


  private var _isServiceCallerCtxBuilt = false
  implicit lazy val serviceCallerContext = _buildServiceCallerContext()

  def runCommand(args: Array[String]): Unit = {

    // Instantiate a JCommander object and set some commands
    val jCmd = new JCommander()
    jCmd.addCommand(RunCortexServiceCommand)

    // Try to parse the command line
    var parsedCommand = ""

    val serviceExecResult = try {
      jCmd.parse(args: _*)
      parsedCommand = jCmd.getParsedCommand()

      if (parsedCommand == null || parsedCommand.isEmpty()) {
        throw new Exception("A command must be provided!")
      } else {
        logger.info(s"Running '$parsedCommand' command...")
      }

      // Execute parsed command
      parsedCommand match {
        case RunCortexServiceCommand.Parameters.firstName => {
          val configSource = scala.io.Source.fromFile(RunCortexServiceCommand.serviceConfig)
          val configAsJsonStr = configSource.getLines().mkString("")
          _runService(RunCortexServiceCommand.serviceName, configAsJsonStr)
        }
        case _ => {
          throw new MissingCommandException(s"Unknown command '${jCmd.getParsedCommand}'")
        }
      }

    } catch {
      case pEx: ParameterException => {
        println()
        logger.warn("Invalid command or parameter", pEx)
        jCmd.usage()

        val jsonErr = new JSONRPC2Error(
          SERVICE_ERROR_CODE,
          "Invalid command or parameter: " + pEx.getMessage
        )
        val resp = ProfiJSONRPC2Response.forError(jsonErr)(1: java.lang.Long)

        System.err.println(resp)
        System.exit(1)
      }
      case t: Throwable => {
        println()
        logger.error(s"Execution of command '$parsedCommand' failed", t)

        val jsonRpcError = _buildJSONRPC2Error(SERVICE_ERROR_CODE,t)
        val resp = ProfiJSONRPC2Response.forError(jsonRpcError)(1: java.lang.Long)

        System.err.println(resp)
        System.exit(1)
      }
    }

    val resp = ProfiJSONRPC2Response(requestId = 1: java.lang.Long, result = serviceExecResult.asInstanceOf[Object])

    System.out.println(resp)
    System.exit(0)
  }

  /*def runService(serviceName: String, serviceConfig: String): Unit = {

    val serviceExecResult = try {
      _runService(serviceName, serviceConfig)
    }
    catch {
      case t: Throwable => {
        println()
        logger.error(s"Execution of cortex service '$serviceName' failed", t)

        val jsonRpcError = _buildJSONRPC2Error(SERVICE_ERROR_CODE,t)
        val resp = ProfiJSONRPC2Response.forError(jsonRpcError)(1: java.lang.Long)

        System.err.println(resp)
        System.exit(1)
      }
    } finally {
      if (_isServiceCallerCtxBuilt) {
        serviceCallerContext.close()
      }
    }

    val resp = BuildJSONRPC2Response(id = 1: java.lang.Long, result = serviceExecResult.asInstanceOf[Object])

    System.out.println(ProfiJson.serialize(resp))
    System.exit(0)
  }*/

  private def _buildJSONRPC2Error(code: Int, t: Throwable): JSONRPC2Error = {

    val stackTrace = t.getStackTrace.map { traceElem =>
      SerializableStackTraceElement(traceElem)
    }

    new JSONRPC2Error(code, t.getMessage, stackTrace)
  }

  private val CORTEX_CLIENT_NAME = "Proline-CLI"

  // TODO: detect first if executed from JAR or not
  lazy val prolineConfig = ConfigFactory.parseFile(new java.io.File("./conf/proline.conf"))
  //ConfigFactory.load(this.getClass.getClassLoader,"application.conf")
  lazy val mountPointPathConverter = MountPointPathConverter(prolineConfig)

  private def _runService(serviceName: String, serviceConfig: String): Any = {

    import com.github.plokhotnyuk.jsoniter_scala.core._
    import com.github.plokhotnyuk.jsoniter_scala.macros._

    // TODO if -f serviceConfig => load serviceConfig from file

    val serviceCall: IServiceCall[Any] = serviceName.replace('_','-') match {
      case "annotate-msms-spectra" =>
        implicit val codec: JsonValueCodec[GenerateSpectrumMatchesCLIConfig] = JsonCodecMaker.make(CodecMakerConfig.withFieldNameMapper(JsonCodecMaker.enforce_snake_case))
        GenerateSpectrumMatches(readFromString[GenerateSpectrumMatchesCLIConfig](serviceConfig))
      case "check-result-files" =>
        CertifyResultFiles(ProfiJson.deserialize[CertifyResultFilesConfig](serviceConfig))
      case "change-repr-protein" =>
        implicit val codec: JsonValueCodec[ChangeRepresentativeProteinMatchConfig] = JsonCodecMaker.make(CodecMakerConfig.withFieldNameMapper(JsonCodecMaker.enforce_snake_case))
        ChangeRepresentativeProteinMatch(readFromString[ChangeRepresentativeProteinMatchConfig](serviceConfig))
      case "delete-orphan-result-sets" =>
        //implicit val codec: JsonValueCodec[DeleteOrphanResultSetsConfig] = JsonCodecMaker.make(CodecMakerConfig.withFieldNameMapper(JsonCodecMaker.enforce_snake_case))
        //DeleteOrphanData.deleteResultSets(readFromString[DeleteOrphanResultSetsConfig](serviceConfig))
        DeleteOrphanData.deleteResultSets(ProfiJson.deserialize[DeleteOrphanResultSetsConfig](serviceConfig))
      case "delete-orphan-result-summaries" =>
        implicit val codec: JsonValueCodec[DeleteOrphanResultSummariesConfig] = JsonCodecMaker.make(CodecMakerConfig.withFieldNameMapper(JsonCodecMaker.enforce_snake_case))
        DeleteOrphanData.deleteResultSummaries(readFromString[DeleteOrphanResultSummariesConfig](serviceConfig))
        //DeleteOrphanData.deleteResultSummaries(ProfiJson.deserialize[DeleteOrphanResultSummariesConfig](serviceConfig))
      case "export-result-summaries" =>
        ExportResultSummaries(ProfiJson.deserialize[ExportResultSummariesConfig](serviceConfig))
      case "import-maxquant-results" =>
        implicit val codec: JsonValueCodec[ImportMaxQuantResultsConfig] = JsonCodecMaker.make(CodecMakerConfig.withFieldNameMapper(JsonCodecMaker.enforce_snake_case))
        ImportMaxQuantResults(ProfiJson.deserialize[ImportMaxQuantResultsConfig](serviceConfig))
      case "import-result-files" =>
        ImportResultFiles(ProfiJson.deserialize[ImportResultFilesCLIConfig](serviceConfig))
      case "generate-msdiag-report" =>
        GenerateMSDiagReport(ProfiJson.deserialize[GenerateMSDiagReportConfig](serviceConfig))
      case "load-fasta-seqs" =>
        //implicit val codec: JsonValueCodec[RetrieveRsmBioSequencesConfig] = JsonCodecMaker.make(CodecMakerConfig.withFieldNameMapper(JsonCodecMaker.enforce_snake_case))
        //RetrieveRsmBioSequences(readFromString[RetrieveRsmBioSequencesConfig](serviceConfig))
        RetrieveRsmBioSequences(ProfiJson.deserialize[RetrieveRsmBioSequencesConfig](serviceConfig))
      case "quantify" =>
        Quantify(ProfiJson.deserialize[QuantifyCLIConfig](serviceConfig))
      case "post-process-quantitation" =>
        //implicit val codec: JsonValueCodec[PostProcessQuantitationConfig] = JsonCodecMaker.make(CodecMakerConfig.withFieldNameMapper(JsonCodecMaker.enforce_snake_case))
        //PostProcessQuantitation(readFromString[PostProcessQuantitationConfig](serviceConfig))
        PostProcessQuantitation(ProfiJson.deserialize[PostProcessQuantitationConfig](serviceConfig))
      case "validate-dataset" =>
        ValidateIdentDSTree(ProfiJson.deserialize[ValidateIdentDSTreeCLIConfig](serviceConfig))
      case _ => throw new Exception(s"Invalid Proline service name '$serviceName'")
    }

    // Await for request to be sent
    Await.result(serviceCall.request, Duration.Inf)
    
    // Await for request to be executed
    Await.result(serviceCall.result, Duration.Inf)
  }

  import javax.jms.JMSContext
  import fr.proline.cortex.api.JMSConnectionConfig
  import fr.proline.cortex.caller.IServiceCallerContext
  import fr.proline.cortex.client.context.BuildJMSContext
  import fr.proline.cortex.client.context.BuildJMSServiceCallerContext

  private def _buildServiceCallerContext(): IServiceCallerContext = {

    println("Working Directory = " + System.getProperty("user.dir"))
    println("Proline Config =\n" +prolineConfig)

    // Update config in SetupProline static object
    SetupProline.setConfigParams(prolineConfig)

    val jmsConnConfigOpt = _tryToLoadJMSConnectionConfig()
    val jmsContextOpt = jmsConnConfigOpt.flatMap(_tryToBuildJMSContext)

    val callerCtx = if (jmsContextOpt.isDefined) {
      BuildJMSServiceCallerContext(Some(CORTEX_CLIENT_NAME), jmsConnConfigOpt.get)(jmsContextOpt.get)
    } else {
      // Instantiate an EmbeddedServiceCallerContext
      _createEmbeddedServiceCallerContext()
    }

    _isServiceCallerCtxBuilt = true

    callerCtx
  }

  private def _createEmbeddedServiceCallerContext(): fr.proline.cortex.runner.EmbeddedServiceCallerContext = {
    fr.proline.cortex.runner.EmbeddedServiceCallerContext(
      Some(CORTEX_CLIENT_NAME),
      DbConnectionHelper.getDataStoreConnectorFactory(),
      prolineConfig,
      mountPointPathConverter
    )
  }

  private def _tryToLoadJMSConnectionConfig(): Option[JMSConnectionConfig] = {
    if (!prolineConfig.hasPath("jms-server")) return None

    try {
      val serverHostName = prolineConfig.getString("jms-server.host")
      //require(serverHostNameOpt.isDefined, "Missing JMS server host name in configuration file")

      require(
        fr.profi.util.StringUtils.isNotEmpty(serverHostName),
        "Incorrect JMS server host name in configuration file (empty)"
      )

      val serverPort = prolineConfig.getInt("jms-server.port")
      //require(serverPortOpt.isDefined, "Missing JMS server port in configuration file")
      require(serverPort > 0, s"Incorrect JMS server port in configuration file (value=$serverPort)")

      Some(JMSConnectionConfig(serverHostName, serverPort))
    } catch {
      case t: Throwable => {
        logger.error("can't load JMS configuration: " + t.getMessage)
        None
      }
    }
  }

  private def _tryToBuildJMSContext(config: JMSConnectionConfig): Option[JMSContext] = {
    try {
      Some(BuildJMSContext(config))
    } catch {
      case t: Throwable => {
        logger.error("can't create a JMSContext because: " + t.getCause.getMessage)
        None
      }
    }
  }


}