package fr.proline.cortex.runner.service.dps.msi

import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.LazyLogging

import fr.proline.core.algo.msi.TypicalProteinChooserRule
import fr.proline.core.service.msi.RsmTypicalProteinChooser
import fr.proline.cortex.api.service.dps.msi.RepresentativeProteinMatchRule

import fr.proline.cortex.caller.IServiceCallerContext
import fr.proline.cortex.client.service.dps.msi.IChangeRepresentativeProteinMatchServiceCaller
import fr.proline.cortex.runner._

case class ChangeRepresentativeProteinMatch()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends IChangeRepresentativeProteinMatchServiceCaller with LazyLogging {

  def apply(
    projectId: Long,
    resultSummaryId: Long,
    rules: Array[RepresentativeProteinMatchRule]
  ): EmbeddedServiceCall[Boolean] = {
    
    RunEmbeddedService {
    
      val rulesForLog = new ArrayBuffer[String](rules.length)
      val allRules = new ArrayBuffer[TypicalProteinChooserRule](rules.length)
      var ruleIndex = 1
  
      val rulesStr = rules.foreach { reprProtMatchRule =>
        
        val ruleName = "R" + ruleIndex
        val ruleOnAc = reprProtMatchRule.ruleOnAc
        val ruleRegex = reprProtMatchRule.ruleRegex
        
        allRules += new TypicalProteinChooserRule(
          ruleName,
          reprProtMatchRule.ruleOnAc,
          reprProtMatchRule.ruleRegex
        )
        
        rulesForLog += s"{rule: $ruleName,onAcc: $ruleOnAc, RegEx: $ruleRegex}"
        
        ruleIndex += 1
      }
  
      val rulesAsStr = rulesForLog.mkString(",\n")
      logger.info(s"Change typical protein using rules: [\n$rulesAsStr]")
      
      callerCtx.inExecutionCtx(projectId, useJPA = true) { execCtx =>
  
        try {
          val typProtChooser = new RsmTypicalProteinChooser(
            execCtx,
            resultSummaryId,
            allRules
          )
          
          typProtChooser.run()
        } catch {
          case t: Throwable => {
            throw new Exception("Can't change the reprensentative protein match in this dataset", t)
          }
        }
      }
      
      true
    }
  }

}