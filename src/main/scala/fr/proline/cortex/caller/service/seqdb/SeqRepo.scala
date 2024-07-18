package fr.proline.cortex.caller.service.seqdb

import javax.jms.TextMessage

import scala.reflect.runtime.universe.typeOf

import fr.profi.util.jsonrpc._
import fr.profi.util.primitives._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.runner._
import fr.proline.jms.service.api._
import fr.proline.jms.util.JMSConstants

case class RetrieveRsmBioSequencesConfig(
  projectId: Long,
  rsmIds: Seq[Long],
  forceUpdate: Option[Boolean]
)

object RetrieveRsmBioSequences {

  def apply(config: RetrieveRsmBioSequencesConfig)(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    this.apply(config.projectId, config.rsmIds, config.forceUpdate.getOrElse(true))
  }

  def apply(projectId: Long, rsmIds: Seq[Long], forceUpdate: Boolean = true)(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    
    val serviceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => SeqRepoService()(jmsCallerCtx)
      case embdCallerCtx: EmbeddedServiceCallerContext => fr.proline.cortex.runner.service.seqdb.SeqRepo()(embdCallerCtx)
    }
    
    serviceCaller.retrieveRsmBioSequences(projectId, rsmIds, forceUpdate)
  }
  
}

trait ISeqRepoServiceCaller extends IServiceCaller {
  def retrieveRsmBioSequences(projectId: Long, rsmIds: Seq[Long], forceUpdate: Boolean): IServiceCall[Boolean]
}

trait ISeqRepoService extends IRemoteServiceIdentity with IDefaultServiceVersion {
  
  /** The namespace of this service like "proline/dps/msi" */
  val serviceNamespace: String = "proline/seq"
  
  /** The name of this service like "ImportResultFiles" */
  // TODO: rename to serviceName when serviceName has been renamed to servicePath
  val serviceLabel: String = "RetrieveBioSeqForRSMs"
  
  /** The description of this service */
  this.serviceDescription = Some(
    "Retrieve BioSequence for all validated proteins of specified Identification Summaries. "+
    "This service will also calculate coverage and protein mass if necessary"
  )
  
  /** List the handled methods **/
  val methodDefinitions: Seq[IJSONRPC2Method] = List(PROCESS_METHOD)

  object PROCESS_METHOD extends JSONRPC2DefaultMethod {

    // Method description
    val name = RemoteServiceIdentity.PROCESS_METHOD_NAME
    val description = "Executes the service. Returns true if operation was successful."
    
    // Configure method interface
    val parameters = List(PROJECT_ID_PARAM, RSM_IDS_PARAM, FORCE_UPDATE_PARAM)
    val returns = JSONRPC2MethodResult(typeOf[Long], "The ID of the created project.")

    object PROJECT_ID_PARAM extends JSONRPC2DefaultMethodParameter {
      val name = "name"
      val description = "The id of the project to get bioSequence for"
      val scalaType = typeOf[String]
    }
    object RSM_IDS_PARAM extends JSONRPC2DefaultMethodParameter {
      val name = "result_summaries_ids"
      val description = "The list of the result summaries to retrieve be bioSequence"
      val scalaType = typeOf[String]
    }
    object FORCE_UPDATE_PARAM extends JSONRPC2DefaultMethodParameter {
      val name = "force_update"
      val description = "specify if biosequences are retrieve and properties are calculate even if they already exit"
      val scalaType = typeOf[String]
      optional = true
    }
  }
  
}

case class SeqRepoService()(implicit val callerCtx: JMSServiceCallerContext) extends AbstractJSONRPC2ServiceCaller
  with ISeqRepoService
  with ISeqRepoServiceCaller {
  
  def retrieveRsmBioSequences(projectId: Long, rsmIds: Seq[Long], forceUpdate: Boolean = true): JMSServiceCall[Boolean] = {
    
    val namedParams: Map[String, Any] = Map(
      "project_id" -> projectId,
      "result_summaries_ids" -> rsmIds,
      "force_update" -> forceUpdate
    )
    
    val annotateRequest = { tm: TextMessage =>
      tm.setStringProperty(JMSConstants.PROLINE_SERVICE_DESCR_KEY, "Retrieve FASTA Sequences")
      ()
    }
    
    // check annotateRequest
    this.executeRemoteMethod(
      methodName = PROCESS_METHOD.name,
      namedParams = Some(namedParams),
      annotateRequest = Some(annotateRequest),
      convertResponseResult = { result => toBoolean(result) }
    )
  }
  
}

