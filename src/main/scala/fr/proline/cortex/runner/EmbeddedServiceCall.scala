package fr.proline.cortex.runner

// FIXME: this file is duplicated in PWX

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import fr.profi.util.misc.InMemoryIdGen
import fr.proline.cortex.caller.IServiceCall

object EmbeddedServiceCall extends InMemoryIdGen

case class EmbeddedServiceCall[+T](
  val requestId: Long,
  val serviceFuture: Future[T]
) extends IServiceCall[T] {
  
  lazy val request: Future[Unit] = Future.successful( () )
  lazy val result: Future[T] = serviceFuture
  
  def getRequestId()(implicit ec: ExecutionContext): Future[String] = {
    Future.successful( requestId.toString )
  }
  
  def map[S](f: T => S)(implicit ec: ExecutionContext): EmbeddedServiceCall[S] = newResult(mapResult(f)(ec))
  
  def newResult[S](newResult: Future[S]): EmbeddedServiceCall[S] = {
    new EmbeddedServiceCall(requestId, newResult)
  }
}