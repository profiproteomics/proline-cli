package fr.proline.cortex.runner

// FIXME: this file is duplicated in PWX

import scala.concurrent.Future

object RunEmbeddedService {
  
  def successful[T](resultValue: T) = {
    EmbeddedServiceCall( EmbeddedServiceCall.generateNewId(), Future.successful(resultValue) )
  }
  
  def apply[T](serviceFn: => T)(implicit embdCallerCtx: EmbeddedServiceCallerContext) = {
    
    implicit val execCtx = embdCallerCtx.threadExecutionContext
    
    val embdServiceCall = EmbeddedServiceCall( EmbeddedServiceCall.generateNewId(), Future {
      
      // Execute the service code
      val result = serviceFn
      
      // Service has finished => we can remove the EmbeddedServiceCall from the registry
     
      // And finally we return the result
      result
    })
    
    embdServiceCall
  }
  
  def apply[T](serviceFuture: Future[T])(implicit embdCallerCtx: EmbeddedServiceCallerContext) = {
    EmbeddedServiceCall(EmbeddedServiceCall.generateNewId(), serviceFuture)
  }
  
}

// TODO: create a registry of running EmbeddedServiceCalls
// We will need to be able to clean it periodically
object EmbddedServiceCallRegistry {
  
}
