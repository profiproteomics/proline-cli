package fr.proline.cortex.caller.service.dps.uds

import fr.profi.util.serialization.ProfiJson
import fr.proline.cortex.api.service.dps.msi.FilterConfig
import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.uds.IValidateIdentDSTreeServiceCaller
import fr.proline.cortex.client.service.dps.uds.ValidateIdentDSTreeConfig
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

case class ValidateIdentDSTreeCLIConfig(
  projectId: Long,
  parentDatasetIds: Array[Long],
  mergeResultSets: Boolean,
  useTdCompetition: Option[Boolean],
  pepMatchFilters: Option[Array[FilterConfig]],
  pepMatchValidatorConfig: Option[PepMatchValidatorConfig],
  pepSetScoreType: Option[String], // TODO: move fr.proline.core.algo.msi.scoring.PepSetScoring to OM
  protSetFilters: Option[Array[FilterConfig]],
  protSetValidatorConfig: Option[ProtSetValidatorConfig]
)

object ValidateIdentDSTree {

  def apply(
    config: ValidateIdentDSTreeCLIConfig
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    this.apply(config.projectId, ProfiJson.getObjectMapper().convertValue(config, classOf[ValidateIdentDSTreeConfig]))
  }
  
  def apply(projectId: Long, config: ValidateIdentDSTreeConfig)(implicit callerCtx: IServiceCallerContext): IServiceCall[Boolean] = {
    val serviceCaller: IValidateIdentDSTreeServiceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.uds.ValidateIdentDSTree()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.uds.ValidateIdentDSTree()(embdCallerCtx)
      }
    }
    serviceCaller.apply(projectId, config)
  }
  
}