package fr.proline.cortex.runner.service.dps.uds

import com.typesafe.scalalogging.LazyLogging

import fr.profi.util.primitives.toLong
import fr.proline.core.service.uds.IdentificationTreeValidator
import fr.proline.cortex.api.service.dps.uds.IValidateIdentDSInTreeService
import fr.proline.cortex.runner.service.dps.msi.ValidateResultSet

import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.uds.IValidateIdentDSTreeServiceCaller
import fr.proline.cortex.client.service.dps.uds.ValidateIdentDSTreeConfig
import fr.proline.cortex.runner._

case class ValidateIdentDSTree()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IValidateIdentDSTreeServiceCaller with LazyLogging {

  def apply(
    projectId: Long,
    config: ValidateIdentDSTreeConfig
  ): EmbeddedServiceCall[Boolean] = {
    
    RunEmbeddedService {
    
      // FIXME: try to use an SQL Execution Context
      callerCtx.inExecutionCtx(projectId, useJPA = true) { execCtx =>
        
        IdentificationTreeValidator.validateIdentificationTrees(
          execCtx,
          config.parentDatasetIds,
          config.mergeResultSets,
          false, //useTdCompet : DEPRECATED 
          ValidateResultSet().buildValidationConfig(config)
        )
        
      }
      
      true
    }
  }
  
}