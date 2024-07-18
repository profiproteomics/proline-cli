package fr.profi.util.jsonrpc

import com.thetransactioncompany.jsonrpc2.JSONRPC2Error
import com.thetransactioncompany.jsonrpc2.JSONRPC2Response
import fr.profi.util.serialization.ProfiJson

object ProfiJSONRPC2Response {

  def apply( result: java.lang.Object, requestId: java.lang.Object ): ProfiJSONRPC2Response  = {
    new ProfiJSONRPC2Response(result, requestId)
  }

  def apply( requestId: java.lang.Object ): ProfiJSONRPC2Response  = {
    new ProfiJSONRPC2Response(requestId)
  }

  def forError(error: JSONRPC2Error)( requestId: java.lang.Object): ProfiJSONRPC2Response  = {
    val resp = new ProfiJSONRPC2Response(requestId)
    resp.setError(error)
    resp
  }

}

/**
 * ProfiJSONRPC2Response uses ProfiJson for JSON response generation.
 *
 * WARN: only use this class for serialization of ProFI "result" objects.
 * For standard or error response use base JSONRPC2Response class.
 * The Response result and id must be serializable via ProfiJson.
 *
 */
class ProfiJSONRPC2Response() extends JSONRPC2Response() {

  def this(result: java.lang.Object, id: java.lang.Object) = {
    this()
    this.setResult(result)
    this.setID(id)
  }

  def this(id: java.lang.Object) = {
    this()
    this.setResult(null)
    this.setID(id)
  }

  protected case class SerializableJSONRPC2Response(
    id: java.lang.Object,
    result: Any,
    error: Option[SerializableJSONRPC2Error] = None,
    jsonrpc: String = "2.0" // JSON RPC version
  )

  protected case class SerializableJSONRPC2Error(
    code: Int,
    message: String,
    data: Any
  )

  /* Override toString instead of toJSONString to be sure that both methods return the same thing */
  override def toString(): String = {
    val resp = SerializableJSONRPC2Response(
      this.getID(),
      this.getResult(),
      Option( this.getError() ).map( e => SerializableJSONRPC2Error(e.getCode(), e.getMessage(), e.getData() ))
    )
    ProfiJson.serialize(resp)
  }

}