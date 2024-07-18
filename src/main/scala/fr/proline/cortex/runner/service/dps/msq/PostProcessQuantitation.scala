package fr.proline.cortex.runner.service.dps.msq

import com.typesafe.scalalogging.LazyLogging

import fr.profi.util.serialization.ProfiJson
import fr.proline.core.algo.msq.config.profilizer.ProfilizerConfig
import fr.proline.core.service.msq.QuantProfilesComputer

import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msq.IPostProcessQuantitationServiceCaller
import fr.proline.cortex.runner._

case class PostProcessQuantitation()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IPostProcessQuantitationServiceCaller with LazyLogging {
  
  def apply(
    projectId: Long,
    masterQuantChannelId: Long,
    config: ProfilizerConfig
  ): EmbeddedServiceCall[Boolean] = {
    
    RunEmbeddedService {
      callerCtx.inExecutionCtx(projectId, useJPA = true) { execCtx =>
  
        val profilizerConfig = ProfiJson.getObjectMapper().convertValue(
          config,
          classOf[fr.proline.core.algo.msq.config.profilizer.ProfilizerConfig]
        )
        
        val quantProfilesComputer = QuantProfilesComputer(
          execCtx,
          masterQuantChannelId,
          profilizerConfig
        )
  
        this.logger.info("Starting QuantProfilesComputer service for Master Quant Channel with id=" + masterQuantChannelId)
        
        quantProfilesComputer.run()
        
        true
      }
    }
  }

}