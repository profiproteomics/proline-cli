package fr.proline.cortex.runner.service.dps.msq

import com.typesafe.scalalogging.LazyLogging

import fr.proline.core.om.model.msq.ExperimentalDesign
import fr.proline.core.om.provider.ProviderDecoratedExecutionContext
import fr.proline.core.om.provider.lcms.IRunProvider
import fr.proline.core.om.provider.lcms.impl.SQLRunProvider
import fr.proline.core.om.provider.lcms.impl.SQLScanSequenceProvider
import fr.proline.core.service.uds.Quantifier

import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msq.IQuantifyServiceCaller
import fr.proline.cortex.client.service.dps.msq.QuantifyConfig
import fr.proline.cortex.runner._

case class Quantify()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IQuantifyServiceCaller with LazyLogging {
  
  private val profiObjectMapper = fr.profi.util.serialization.ProfiJson.getObjectMapper()
  
  def apply(
    projectId: Long,
    config: QuantifyConfig
  ): EmbeddedServiceCall[Long] = {
   
    RunEmbeddedService {
      callerCtx.inExecutionCtx(projectId, useJPA = true) { execCtx =>
        
        // Register SQLRunProvider 
        val scanSeqProvider = new SQLScanSequenceProvider(execCtx.getLCMSDbConnectionContext())
        val lcMsRunProvider = new SQLRunProvider(
          execCtx.getUDSDbConnectionContext(),
          Some(scanSeqProvider),
          Some(callerCtx.mountPointPathConverter)
        )
        val providerContext = ProviderDecoratedExecutionContext(execCtx) // Use Object factory
        providerContext.putProvider(classOf[IRunProvider], lcMsRunProvider)
        
        // TODO: remove me, temporary workarounds used until configuration files have been revised (see #15945,#15948)
        /*fr.proline.core.service.lcms.io.ExtractMapSet.setMzdbMaxParallelism(
          NodeConfig.MZDB_MAX_PARALLELISM
        )
        fr.proline.core.service.lcms.io.ExtractMapSet.setTempDirectory(
          new java.io.File(NodeConfig.PEAKELDB_TEMP_DIRECTORY)
        )*/
      
        val quantifier = new Quantifier(
          executionContext = providerContext,
          name = config.name,
          description = config.description,
          projectId = projectId,
          methodId = config.methodId,
          experimentalDesign = config.experimentalDesign,
          quantConfigAsMap = profiObjectMapper.convertValue(config.quantitationConfig, classOf[java.util.Map[String,Object]])
        )
        quantifier.run()
  
        quantifier.getQuantitationId
      }
    }
  }

}