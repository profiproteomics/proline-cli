package fr.proline.cortex.runner.service.seqdb

import java.io.File

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.StrictLogging

import jnr.ffi._

import fr.proline.core.om.model.msi.Protein
import fr.proline.core.om.model.msi.ProteinMatchProperties
import fr.proline.core.orm.msi.{
  Alphabet,
  BioSequence => MsiBioSequence,
  Enzyme => MsiEnzyme,
  ResultSummary => MsiResultSummary
}
import fr.proline.cortex.caller.service.seqdb.ISeqRepoServiceCaller
import fr.proline.cortex.runner._
import fr.profi.chemistry.algo.DigestionUtils
import fr.profi.chemistry.model.Enzyme
import fr.profi.chemistry.model.EnzymeCleavage
import fr.profi.util.collection._
import fr.profi.util.serialization.ProfiJson

private[this] case class ProteinRegion(
  resBefore: Char,
  resAfter: Char,
  start: Int,
  stop: Int
) {
  def boundaries(): (Int, Int) = (start, stop)
}

private[this] case class ProteinMatchRef(
  id: Long,
  accession: String,
  seqDbId: Long,
  resultSetId: Long,
  coveredSequenceRegions: ArrayBuffer[ProteinRegion] = new ArrayBuffer[ProteinRegion]
) {
  
  // FIXME: remplace Protein.calcSequenceCoverage() by this new version
  def calcSequenceCoverage(protSeqLength: Int): Float = {

    // Map sequence positions
    val seqIndexSet = new java.util.HashSet[Int]()
    for (seqPosition <- coveredSequenceRegions.map(_.boundaries)) {
      for (seqIdx <- seqPosition._1 to seqPosition._2) {
        seqIndexSet.add(seqIdx)
      }
    }

    val coveredSeqLength = seqIndexSet.size().toFloat
    val coverage = 100 * coveredSeqLength / protSeqLength

    coverage
  }
}

private[this] case class BioSeqMatch(
  proteinMatchId: Long,
  description: String,
  geneName: Option[String],
  sequence: String,
  length: Int,
  crc64: String,
  mass: Float,
  pi: Float,
  coverage: Float,
  coveredSequenceRegions: Seq[ProteinRegion],
  observablePeptideCount: Int
)

private[this] case class BioSeqRef(
  id: Long,
  length: Int,
  crc64: String,
  mass: Float
)

case class SeqRepo()(implicit val callerCtx: EmbeddedServiceCallerContext)
  extends ISeqRepoServiceCaller with StrictLogging {
  
  private val GeneRegex = ".*GN=([^\\s]+).*".r
  
  private val fastaLib = LibraryLoader.create(classOf[FastaLib]).load(
    "libproteomics_fasta.dll"
  )
  
  def retrieveRsmBioSequences(projectId: Long, rsmIds: Seq[Long], forceUpdate: Boolean = true): EmbeddedServiceCall[Boolean] = {
    RunEmbeddedService.apply {
      _retrieveRsmBioSequences(projectId, rsmIds, forceUpdate)
    }
  }
  
  private def _retrieveRsmBioSequences(projectId: Long, rsmIds: Seq[Long], forceUpdate: Boolean): Boolean = {
    
    val bioSeqRefs = new ArrayBuffer[BioSeqRef](10000)
    
    val rsIdByRsmId = callerCtx.inMsiDbCtx(projectId, { msiDbCtx =>
      
      fr.proline.core.dal.DoJDBCReturningWork.withEzDBC(msiDbCtx) { ezDBC =>
        val bioSeqRefQuery = "SELECT id, length, crc64, mass FROM bio_sequence"
        ezDBC.selectAndProcess(bioSeqRefQuery) { r =>
          bioSeqRefs += BioSeqRef(r.nextLong, r.nextInt, r.nextString, r.nextFloat)
        }
      }
      
      new fr.proline.core.dal.helper.MsiDbHelper(msiDbCtx).getResultSetIdByResultSummaryId(rsmIds)
    })
    
    val bioSeqRefByCrc64 = bioSeqRefs.map(bsr => bsr.crc64 -> bsr).toMap
    assert(bioSeqRefByCrc64.size == bioSeqRefs.length, "crc64 collision detected between protein sequences")
      
    for (rsmId <- rsmIds) {
      val seqDbIdSet = new collection.mutable.HashSet[Long]()
      
      val rsId = rsIdByRsmId(rsmId)
      
      val protMatchRefsBySeqDbId = callerCtx.withMsiEzDBC(projectId, { ezDBC =>
        
        /* Retrieve validated peptides IDs */
        val validatedPeptideIdQuery = s"SELECT peptide_id FROM peptide_instance WHERE result_summary_id = $rsmId"
        val validatedPeptideIdMap = ezDBC.selectLongs(validatedPeptideIdQuery).toLongMapWith { id =>
          (id, true)
        }
        
        /* Retrieve information about validated protein matches */
        val protMatchQuery = "SELECT protein_match.id, accession, seq_database_id, protein_match.result_set_id "+
          "FROM protein_set_protein_match_item, protein_match, protein_match_seq_database_map "+
          s"WHERE protein_set_protein_match_item.result_summary_id = $rsmId "+
          "AND protein_set_protein_match_item.protein_match_id = protein_match.id "+
          "AND protein_match.id = protein_match_seq_database_map.protein_match_id"
          //println(sqlQuery)
          
        val protMatchRefs = ezDBC.select(protMatchQuery) { r =>
          val protMatchRef = ProteinMatchRef(r.nextLong,r.nextString,r.nextLong,r.nextLong)
          seqDbIdSet += protMatchRef.seqDbId
          protMatchRef
        }
        val protMatchRefById = protMatchRefs.mapByLong(_.id)
        
        /* Retrieve information about sequence matches (to compute sequence coverage, see below) */
        //val rsIds = protMatchRefs.map(_.resultSetId).distinct
        val seqMatchesQuery = s"SELECT protein_match_id, peptide_id, residue_before, residue_after, start, stop FROM sequence_match WHERE result_set_id = $rsId"
        ezDBC.selectAndProcess(seqMatchesQuery) { r =>
          val protMatchId = r.nextLong
          val protMatchRefOpt = protMatchRefById.get(protMatchId)
          
          if (protMatchRefOpt.isDefined) {
            val peptideId = r.nextLong
            if (validatedPeptideIdMap.contains(peptideId)) {
              val resBefore = r.nextString.charAt(0)
              val resAfter = r.nextString.charAt(0)
              val start = r.nextInt
              val stop = r.nextInt
              
              protMatchRefOpt.get.coveredSequenceRegions += ProteinRegion(resBefore, resAfter, start, stop)
            }
          }
        }
        
        protMatchRefs.groupByLong(_.seqDbId)
      })
      
      //println(protMatchRefs.length)
      //println(seqDbIdSet)
      
      callerCtx.inExecutionCtx(projectId, useJPA = true) { execCtx =>
        
        /*val msiSearchProvider = new fr.proline.core.om.provider.msi.impl.SQLMsiSearchProvider(
          execCtx.getUDSDbConnectionContext,
          execCtx.getMSIDbConnectionContext
        )
        val seqDbs = msiSearchProvider.getSeqDatabases(seqDbIdSet.toSeq)
        val fastaFilePaths = seqDbs.map(_.filePath)*/
        
        val msiEM = execCtx.getMSIDbConnectionContext.getEntityManager
        
        val msiRsm = msiEM.find(classOf[fr.proline.core.orm.msi.ResultSummary], rsmId)
        val enzymeOpt = this._getMsiSearchEnzyme(msiRsm)(execCtx)
        
        if (enzymeOpt.isEmpty) {
          logger.warn("Can't find enzyme definition for result summary with ID=" + rsmId)
        } else {
          
          val bioSeqMatches = new ArrayBuffer[BioSeqMatch](protMatchRefsBySeqDbId.values.map(_.length).sum)
          
          seqDbIdSet.foreach { seqDbId =>
            val msiSeqDb = msiEM.find(classOf[fr.proline.core.orm.msi.SeqDatabase], seqDbId)
            val fastaFilePath = msiSeqDb.getFastaFilePath
            val fastaFile = new File(fastaFilePath)
            if (!fastaFile.isFile) {
              logger.error("Can't access FASTA file at: "+ fastaFilePath)
              return false
            }
            
            val protMatches = protMatchRefsBySeqDbId(seqDbId)
            val protMatchByAccession = protMatches.map(pm => pm.accession -> pm).toMap
            val accessions = protMatchByAccession.keys.toArray //protMatches.map(_.accession).toArray
            logger.debug(s"Searching for ${accessions.length} protein sequences in FASTA file: "+fastaFilePath)
            
            val raf = new java.io.RandomAccessFile(fastaFile, "r")
            try {
              val resultCb = new Object with SearchAccessionsCallback {
                def onResult(accession: String, pos: Int, len: Int): Unit = {
                  println(s"Found accession '$accession' at $pos:$len")
              
                  val entryChars = new Array[Byte](len)
                  raf.seek(pos)
                  raf.read(entryChars)
              
                  val fastaEntry = new String(entryChars) //.trim()
                  //println(fastaEntry)
                  
                  val fastaEntryParts = fastaEntry.split("\n").map(_.trim)
                  val fastaHead = fastaEntryParts.head
                  if (!fastaHead.contains("REVERSED|")) {
                    
                    val description = fastaHead.substring(1)
                    val geneNameOpt = description match {
                      case GeneRegex(geneName) => Some(geneName)
                      case _ => None
                    }
                    
                    val sequence = fastaEntryParts.tail.mkString
                    val seqLen = sequence.length
                    
                    val protMatch = protMatchByAccession(accession)
                    
                    val bioSeqMatch = BioSeqMatch(
                      proteinMatchId = protMatch.id,
                      description = description,
                      geneName = geneNameOpt,
                      sequence = sequence,
                      length = seqLen,
                      crc64 = Protein.calcCRC64(sequence),
                      mass = Protein.calcMass(sequence).toFloat,
                      pi = Protein.calcPI(sequence),
                      coverage = protMatch.calcSequenceCoverage(seqLen),
                      coveredSequenceRegions = protMatch.coveredSequenceRegions,
                      observablePeptideCount = DigestionUtils.getObservablePeptidesCount(
                        sequence,
                        enzymeOpt.get
                      )
                    )
                    bioSeqMatches += bioSeqMatch
                    
                    //println(protMatch.id)
                  }
                  
                }
              } // ends SearchAccessionsCallback
              
              fastaLib.search_accessions(fastaFilePath, accessions.toArray, accessions.length, resultCb)
            } finally {
              raf.close()
            }
  
          } // ends FASTA files lookup
          
          val bioSeqMatchesByProtMatchId = bioSeqMatches.groupByLong(_.proteinMatchId)
          
          val msiProtMatches = fr.proline.core.orm.msi.repository.ProteinMatchRepository.findProteinMatchesForResultSetId(msiEM, rsId)
          
          msiEM.getTransaction().begin()
          
          msiProtMatches.foreach { msiProtMatch: fr.proline.core.orm.msi.ProteinMatch =>
            val bioSeqMatches = bioSeqMatchesByProtMatchId.getOrElse(msiProtMatch.getId,Seq())
            
            // When multiple hits => keep only biological sequences which are consistent with the protein regions of identified peptides
            val filteredBioSeqMatches = if (bioSeqMatches.length <= 1) bioSeqMatches
            else bioSeqMatches.toSeq.filter { bioSeqMatch =>
              val seq = bioSeqMatch.sequence
              val arePeptidesMatching = bioSeqMatch.coveredSequenceRegions.forall { region =>
                
                val resBefore = region.resBefore
                val isResBeforeMatching = if (resBefore == '-' || region.start < 0) true
                else if (region.start - 2 < 0 || region.start - 2 >= seq.length) false
                else {
                  //println(seq.charAt(region.start - 2), resBefore)
                  seq.charAt(region.start - 2) == resBefore
                }
                
                val resAfter = region.resAfter
                val isResAfterMatching = if (resAfter == '-') true
                else if (region.stop < 0 || region.stop >= seq.length) false
                else {
                  //println(seq.charAt(region.stop) , resAfter)
                  seq.charAt(region.stop) == resAfter
                }
                
                isResBeforeMatching && isResAfterMatching
              }
              
              arePeptidesMatching
            }
            
            
            if (filteredBioSeqMatches.length > 1) {
              logger.warn("Found multiple FASTA entries matching the accession protein match " + msiProtMatch.getAccession)
            }
            
            if (filteredBioSeqMatches.isEmpty) {
              logger.warn(s"Can't update protein match, no consistent sequence among the ${bioSeqMatches.length} FASTA entries matching the accession protein match " + msiProtMatch.getAccession)
            } else {
              
              val bioSeqMatch = filteredBioSeqMatches.head
              
              def bioSeqMatch2bioSeq(msiBioSeq: MsiBioSequence) {
                msiBioSeq.setAlphabet(Alphabet.AA)
                msiBioSeq.setSequence(bioSeqMatch.sequence)
                msiBioSeq.setLength(bioSeqMatch.length)
                msiBioSeq.setCrc64(bioSeqMatch.crc64)
                msiBioSeq.setMass(bioSeqMatch.mass.toInt)
                msiBioSeq.setPi(bioSeqMatch.pi)
              }
              
              if (msiProtMatch.getBioSequenceId == null) {
                val existingBioSeqRefOpt = bioSeqRefByCrc64.get(bioSeqMatch.crc64)
                
                if (existingBioSeqRefOpt.isDefined) {
                  msiProtMatch.setBioSequenceId(existingBioSeqRefOpt.get.id)
                } else {
                  val bioSeqId = - msiProtMatch.getId
                  msiProtMatch.setBioSequenceId(bioSeqId)
                  
                  val msiBioSeq = new MsiBioSequence()
                  msiBioSeq.setId(bioSeqId)
                  bioSeqMatch2bioSeq(msiBioSeq)
                  msiEM.persist(msiBioSeq)
                }

              } else if (forceUpdate) {
                println("Updating bio sequence of protein match " + msiProtMatch.getAccession)
                val msiBioSeq = msiEM.find(
                  classOf[fr.proline.core.orm.msi.BioSequence],
                  msiProtMatch.getBioSequenceId.longValue()
                )
                
                bioSeqMatch2bioSeq(msiBioSeq)
                msiEM.merge(msiBioSeq)
              }
              
              if (bioSeqMatch.geneName.isDefined) {
                msiProtMatch.setGeneName(bioSeqMatch.geneName.get)
              }
              
              if (forceUpdate) {
                msiProtMatch.setDescription(bioSeqMatch.description)
              }
              
              val protMatchProps = Option(msiProtMatch.getSerializedProperties())
                .map(ProfiJson.deserialize[ProteinMatchProperties])
                .getOrElse(new ProteinMatchProperties())
              protMatchProps.setObservablePeptideCount(bioSeqMatch.observablePeptideCount)
              
              msiProtMatch.setSerializedProperties(
                ProfiJson.serialize(protMatchProps)
              )
              
              msiEM.merge(msiProtMatch)
              
              val msiProtSetProtMatchMappings = msiProtMatch.getProteinSetProteinMatchItems.filter(_.getResultSummary.getId == rsmId)
              if (msiProtSetProtMatchMappings.size != 1) {
                logger.warn(s"${msiProtMatch.getAccession} is found in ${msiProtSetProtMatchMappings.size} protein sets")
              }
              
              msiProtSetProtMatchMappings.foreach { msiProtSetProtMatchMapping =>
                val msiProtSetProtMatchMapping = msiProtSetProtMatchMappings.head
                msiProtSetProtMatchMapping.setCoverage(bioSeqMatch.coverage)
                
                msiEM.merge(msiProtSetProtMatchMapping)
              }
              
            }
          }
          
          /*
           *
           * // FIXME: Save RSM Property
             JsonObject array = getPropertiesAsJsonObject(rsm.getSerializedProperties());
             if (!array.has("is_coverage_updated")) {
               LOG.debug(" Saving coverage_updated property for rsm {}.", rsmId);
               array.addProperty("is_coverage_updated", true);
               rsm.setSerializedProperties(array.toString());
               msiEM.merge(rsm);
             }
           */
          
          msiEM.getTransaction().commit()
           
        } // ends if if (enzymeOpt.isDefined)
        
      }
      
    } // ends for loop
    
    
    true
  }
  
  private def _getMsiSearchEnzyme(msiRsm: MsiResultSummary)(execCtx: fr.proline.context.IExecutionContext): Option[Enzyme] = {
    
    // FIXME: for old quant we need to retrieve the correct mapping from the UDS DB
    
    // SQL query for a specific RSM of a given project
    // SELECT quant_result_summary_id, quant_channel.ident_result_summary_id
    // FROM quant_channel, master_quant_channel WHERE master_quant_channel.quantitation_id 
    // IN (SELECT id FROM data_set WHERE project_id = $projectId and type = 'QUANTITATION') 
    // AND master_quant_channel.quant_result_summary_id = 549 AND master_quant_channel.id = quant_channel.master_quant_channel_id 
    // ORDER BY master_quant_channel_id ASC, quant_channel.number ASC
    
    // SQL query for all RSMs of a given project
    // SELECT quant_result_summary_id, quant_channel.ident_result_summary_id FROM quant_channel, master_quant_channel WHERE quant_channel.quantitation_id 
    // IN (SELECT id FROM data_set WHERE project_id = 6 and type = 'QUANTITATION') AND master_quant_channel.id = quant_channel.master_quant_channel_id ORDER BY master_quant_channel_id ASC, quant_channel.number ASC
    
    import java.util
    //import fr.proline.core.orm.msi.repository.ResultSetRepository
    
    val udsEM = execCtx.getUDSDbConnectionContext.getEntityManager
    
    var enzyme: Enzyme = null
    val msiSearch = msiRsm.getResultSet.getMsiSearch
    var foundMsiEnzymes: util.Set[MsiEnzyme] = null
    var isSameDigestion = true
    
    if (msiSearch != null) {
      foundMsiEnzymes = msiSearch.getSearchSetting.getEnzymes
    } else {
      // Look in child and verify same enzyme was used for all children
      /*val childSearchesIds = ResultSetRepository.findChildMsiSearchIdsForResultSet(
        msiEM,
        msiRsm.getResultSet.getId
      )*/
      
      val childMsiSearches = msiRsm.getChildren.map(_.getResultSet.getMsiSearch)
      
      for (childMsiSearch <- childMsiSearches; if isSameDigestion) {
        val enzymes = childMsiSearch.getSearchSetting.getEnzymes
        if (foundMsiEnzymes == null)
          foundMsiEnzymes = enzymes
          
        if (!foundMsiEnzymes.equals(enzymes)) {
          isSameDigestion = false    
        }
      }
    }
    
    // LOG.warn("Can't get Enzyme for Merged ResultSet as child don't have the same Enzyme ! ")
    if (!isSameDigestion || foundMsiEnzymes == null || foundMsiEnzymes.isEmpty) None
    else {
      // VDS FIXME: Why use first
      val udsEnzyme = udsEM.find(classOf[fr.proline.core.orm.uds.Enzyme], foundMsiEnzymes.head.getId)
      
      val cleavages = udsEnzyme.getCleavages.map { c =>
        EnzymeCleavage(
          c.getId,
          c.getSite,
          c.getResidues,
          Option(c.getRestrictiveResidues)
        )
      }
      
      Some(
        Enzyme(
          udsEnzyme.getId,
          udsEnzyme.getName,
          cleavages.toArray,
          Option(udsEnzyme.getCleavageRegexp),
          isIndependant = false,
          isSemiSpecific = false,
          properties = None
        )
      )
    }
    
  }
  
}

trait SearchAccessionsCallback {
  @jnr.ffi.annotations.Delegate
  def onResult(accession: String, position: Int, len: Int): Unit
}

trait FastaLib {
  def search_accessions(fastaPath: String, accessions: Array[String], n: Int, cb: SearchAccessionsCallback): Boolean
}

