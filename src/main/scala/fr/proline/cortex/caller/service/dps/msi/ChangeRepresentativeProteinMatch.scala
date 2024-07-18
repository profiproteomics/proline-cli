package fr.proline.cortex.caller.service.dps.msi

import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IChangeRepresentativeProteinMatchServiceCaller
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

case class ChangeRepresentativeProteinMatchConfig(
  projectId: Long,
  resultSummaryId: Long,
  rules: Array[RepresentativeProteinMatchRule]
)

object ChangeRepresentativeProteinMatch {

  def apply(config: ChangeRepresentativeProteinMatchConfig)(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    this.apply(config.projectId, config.resultSummaryId, config.rules)
  }
  
  def apply(
    projectId: Long,
    resultSummaryId: Long,
    rules: Array[RepresentativeProteinMatchRule]
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    val serviceCaller: IChangeRepresentativeProteinMatchServiceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msi.ChangeRepresentativeProteinMatch()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msi.ChangeRepresentativeProteinMatch()(embdCallerCtx)
      }
    }
    serviceCaller.apply(projectId, resultSummaryId, rules)
  }
  
}