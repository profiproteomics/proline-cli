package fr.proline.cortex.caller.service.dps.msi

import fr.proline.cortex.api.service.dps.msi._
import fr.proline.cortex.caller._
import fr.proline.cortex.client._
import fr.proline.cortex.client.context.JMSServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IImportValidateGenerateSMServiceCaller
import fr.proline.cortex.client.service.dps.msi.ImportResultFileConfig
import fr.proline.cortex.client.service.dps.msi.ValidateResultSetConfig
import fr.proline.cortex.runner.EmbeddedServiceCallerContext

object ImportValidateGenerateSM {
  
  def apply(
    projectId: Long,
    
    // Import specific parameters
    importResultFileConfig: ImportResultFileConfig,
    
    // Validation specific parameters
    validateResultSetConfig: ValidateResultSetConfig,
    
    // "Generate Spectrum Matches" specific parameters
    generateSpectrumMatches: Option[Boolean] = None,
    forceInsert: Option[Boolean] = None
    
  )(implicit callerCtx: IServiceCallerContext): IServiceCall[Array[ImportedResultFile]] = {
    val serviceCaller = callerCtx match {
      case jmsCallerCtx: JMSServiceCallerContext => {
        fr.proline.cortex.client.service.dps.msi.ImportValidateGenerateSM()(jmsCallerCtx)
      }
      case embdCallerCtx: EmbeddedServiceCallerContext => {
        fr.proline.cortex.runner.service.dps.msi.ImportValidateGenerateSM()(embdCallerCtx)
      }
    }
    serviceCaller.apply(projectId, importResultFileConfig, validateResultSetConfig, generateSpectrumMatches, forceInsert)
  }
  
}