package fr.proline.core.orm.uds;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.MapKeyColumn;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.Table;
import javax.persistence.Transient;

import fr.profi.util.StringUtils;
import fr.proline.core.orm.msi.ResultSet;
import fr.proline.core.orm.msi.ResultSummary;

/**
 * The persistent class for the dataset database table.
 * 
 */
@Entity
@Table(name = "data_set")
@Inheritance(strategy = InheritanceType.JOINED)
@NamedQueries({
	@NamedQuery(name = "findDatasetByProject", query = "Select ds from Dataset ds where ds.project.id =:id"),
	@NamedQuery(name = "findRootDatasetByProject", query = "Select ds from Dataset ds where ds.project.id =:id and ds.parentDataset is null"),
	@NamedQuery(name = "findDatasetNamesByProject", query = "Select ds.name from Dataset ds where ds.project.id =:id"),
	@NamedQuery(name = "findRootDatasetNamesByProject", query = "Select ds.name from Dataset ds where ds.project.id =:id and ds.parentDataset is null"),
	@NamedQuery(name = "findDatasetByNameAndProject", query = "Select ds from Dataset ds where ds.project.id =:id and ds.name=:name"),
	@NamedQuery(name = "findRootDatasetByNameAndProject", query = "Select ds from Dataset ds where ds.project.id =:id and ds.name=:name and ds.parentDataset is null")
})
public class Dataset implements Serializable {

	private static final long serialVersionUID = 1L;

	public enum DatasetType {
		IDENTIFICATION, QUANTITATION, AGGREGATE, TRASH, QUANTITATION_FOLDER, IDENTIFICATION_FOLDER
	};

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private long id;

	@Column(name = "creation_timestamp")
	private Timestamp creationTimestamp = new Timestamp(new Date().getTime());

	private String description;

	@Column(name = "child_count")
	private int childCount;

	private String keywords;

	@Column(name = "modification_log")
	private String modificationLog;

	private String name;

	@Enumerated(value = EnumType.STRING)
	private DatasetType type;

	private int number;

	@ManyToOne
	@JoinColumn(name = "fractionation_id")
	private Fractionation fractionation;

	@ManyToOne
	@JoinColumn(name = "aggregation_id")
	private Aggregation aggregation;

	@ManyToOne
	@JoinColumn(name = "project_id")
	private Project project;

	@Column(name = "serialized_properties")
	private String serializedProperties;

	// bi-directional many-to-one association to DataSet
	@ManyToOne
	@JoinColumn(name = "parent_dataset_id")
	private Dataset parentDataset;

	// bi-directional many-to-one association to DataSet
	@OneToMany(mappedBy = "parentDataset")
	@OrderBy("number")
	private List<Dataset> children;

	@Column(name = "result_set_id")
	private Long resultSetId;

	@Column(name = "result_summary_id")
	private Long resultSummaryId;

	// bi-directional many-to-one association to BiologicalSample
	@OneToMany(mappedBy = "dataset")
	@OrderBy("number")
	private List<BiologicalSample> biologicalSamples;

	// bi-directional many-to-one association to GroupSetup
	@OneToMany(mappedBy = "dataset")
	private Set<GroupSetup> groupSetups;

	// bi-directional many-to-one association to QuantChannel
	@OneToMany(mappedBy = "dataset")
	@OrderBy("number")
	private List<QuantitationChannel> quantitationChannels;

	// uni-directional many-to-one association to QuantMethod
	@ManyToOne
	@JoinColumn(name = "quant_method_id")
	private QuantitationMethod method;

	// bi-directional many-to-one association to MasterQuantitationChannel
	@OneToMany(mappedBy = "quantDataset")
	@OrderBy("number")
	private List<MasterQuantitationChannel> masterQuantitationChannels;

	// bi-directional many-to-one association to SampleAnalysis
	@OneToMany(mappedBy = "dataset")
	private Set<SampleAnalysis> sampleAnalyses;

	@ElementCollection
	@MapKeyColumn(name = "schema_name")
	@Column(name = "object_tree_id")
	@CollectionTable(name = "data_set_object_tree_map", joinColumns = @JoinColumn(name = "data_set_id", referencedColumnName = "id") )
	private Map<String, Long> objectTreeIdByName;

	// Transient Variables not saved in database
	@Transient
	private TransientData transientData = null;

	protected Dataset() {
	}

	public Dataset(Project project) {
		this.project = project;
	}

	public long getId() {
		return id;
	}

	public void setId(final long pId) {
		id = pId;
	}

	public int getChildrenCount() {
		return childCount;
	}

	public void setChildrenCount(final int pChildrenCount) {
		childCount = pChildrenCount;
	}

	public Timestamp getCreationTimestamp() {
		Timestamp result = null;

		if (creationTimestamp != null) { // Should not be null
			result = (Timestamp) creationTimestamp.clone();
		}

		return result;
	}

	public void setCreationTimestamp(final Timestamp pCreationTimestamp) {

		if (pCreationTimestamp == null) {
			throw new IllegalArgumentException("PCreationTimestamp is null");
		}

		creationTimestamp = (Timestamp) pCreationTimestamp.clone();
	}

	public String getDescription() {
		return this.description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public DatasetType getType() {
		return type;
	}

	public void setType(DatasetType type) {
		this.type = type;
	}

	public Dataset getParentDataset() {
		return parentDataset;
	}

	public void setParentDataset(Dataset parentDataset) {
		this.parentDataset = parentDataset;
	}

	public Long getResultSetId() {
		return resultSetId;
	}

	public void setResultSetId(final Long pResultSetId) {
		resultSetId = pResultSetId;
	}

	public Long getResultSummaryId() {
		return resultSummaryId;
	}

	public void setResultSummaryId(final Long pResultSummaryId) {
		resultSummaryId = pResultSummaryId;
	}

	public String getKeywords() {
		return this.keywords;
	}

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	public String getModificationLog() {
		return this.modificationLog;
	}

	public void setModificationLog(String modificationLog) {
		this.modificationLog = modificationLog;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getNumber() {
		return number;
	}

	public void setNumber(final int pNumber) {
		number = pNumber;
	}

	public Project getProject() {
		return this.project;
	}

	public void setProject(Project project) {
		this.project = project;
	}

	public String getSerializedProperties() {
		return this.serializedProperties;
	}

	public void setSerializedProperties(String serializedProperties) {
		this.serializedProperties = serializedProperties;
	}

	public List<BiologicalSample> getBiologicalSamples() {
		return biologicalSamples;
	}

	public void setBiologicalSamples(final List<BiologicalSample> biologicalSamples) {
		this.biologicalSamples = biologicalSamples;
	}

	public Set<GroupSetup> getGroupSetups() {
		return this.groupSetups;
	}

	public void setGroupSetups(Set<GroupSetup> groupSetups) {
		this.groupSetups = groupSetups;
	}

	public List<QuantitationChannel> getQuantitationChannels() {
		return quantitationChannels;
	}

	public void setQuantitationChannels(final List<QuantitationChannel> quantitationChannels) {
		this.quantitationChannels = quantitationChannels;
	}

	public QuantitationMethod getMethod() {
		return this.method;
	}

	public void setMethod(QuantitationMethod method) {
		this.method = method;
	}

	public List<MasterQuantitationChannel> getMasterQuantitationChannels() {
		return masterQuantitationChannels;
	}

	public void setMasterQuantitationChannels(final List<MasterQuantitationChannel> masterQuantitationChannels) {
		this.masterQuantitationChannels = masterQuantitationChannels;
	}

	public Set<SampleAnalysis> getSampleReplicates() {
		return sampleAnalyses;
	}

	public void setSampleReplicates(final Set<SampleAnalysis> sampleAnalyses) {
		this.sampleAnalyses = sampleAnalyses;
	}

	public Fractionation getFractionation() {
		return fractionation;
	}

	public void setFractionation(Fractionation fractionation) {
		this.fractionation = fractionation;
	}

	public Aggregation getAggregation() {
		return aggregation;
	}

	public void setAggregation(Aggregation aggregation) {
		this.aggregation = aggregation;
	}

	void setObjectTreeIdByName(final Map<String, Long> objectTree) {
		objectTreeIdByName = objectTree;
	}

	public Map<String, Long> getObjectTreeIdByName() {
		return objectTreeIdByName;
	}

	public Long putObject(final String schemaName, final Long objectId) {
		if (StringUtils.isEmpty(schemaName)) {
			throw new IllegalArgumentException("Invalid schemaName");
		}

		Map<String, Long> localObjectTree = getObjectTreeIdByName();

		if (localObjectTree == null) {
			localObjectTree = new HashMap<String, Long>();

			setObjectTreeIdByName(localObjectTree);
		}

		return localObjectTree.put(schemaName, Long.valueOf(objectId));
	}

	public Long removeObject(final String schemaName) {
		Long result = null;

		final Map<String, Long> localObjectTree = getObjectTreeIdByName();
		if (localObjectTree != null) {
			result = localObjectTree.remove(schemaName);
		}
		return result;
	}

	public List<Dataset> getChildren() {
		return children;
	}

	public void setChildren(final List<Dataset> children) {
		this.children = children;
	}

	public void addChild(Dataset child) {
		List<Dataset> childrenList = getChildren();
		if (childrenList == null) {
			childrenList = new ArrayList<Dataset>(1);
			setChildren(childrenList);
		}
		childrenList.add(child);
		child.setNumber(childCount);
		childCount++;
		child.setParentDataset(this);
	}

	public void insertChild(Dataset child, int index) {
		List<Dataset> childrenList = getChildren();
		if (childrenList == null) {
			childrenList = new ArrayList<Dataset>(1);
			setChildren(childrenList);
		}
		childrenList.add(index, child);
		child.setNumber(index);
		childCount++;

		for (int i = index + 1; i < childrenList.size(); i++) {
			childrenList.get(i).setNumber(i);
		}
		child.setParentDataset(this);
	}

	public void replaceAllChildren(List<Dataset> newChildren) {
		List<Dataset> childrenList = getChildren();
		if (childrenList != null) {
			Iterator<Dataset> it = childrenList.iterator();
			while (it.hasNext()) {
				Dataset child = it.next();
				child.setParentDataset(null);
				child.setNumber(0);
			}
			childrenList.clear();
			childCount = 0;
		}
		Iterator<Dataset> it = newChildren.iterator();
		while (it.hasNext()) {
			Dataset newChild = it.next();
			addChild(newChild);
		}
	}

	public Set<IdentificationDataset> getIdentificationDataset() {
		Set<IdentificationDataset> idfDS = new HashSet<IdentificationDataset>();
		if ((getChildren() == null || getChildren().isEmpty())
			&& (getType().equals(DatasetType.IDENTIFICATION))) {
			idfDS.add((IdentificationDataset) this);
		}

		for (Dataset ds : getChildren()) {
			idfDS.addAll(ds.getIdentificationDataset());
		}
		return idfDS;

	}

	// FIXME: (IdentificationDataset)this in getIdentificationDataset throws a ClassCastException
	// this method is just a workaround but a better solution has to be implemented
	public Set<Dataset> getIdentificationDatasets() {
		
		List<Dataset> children = getChildren();
		
		Set<Dataset> idfDS = new HashSet<Dataset>();
		if ((children == null || children.isEmpty()) && (getType().equals(DatasetType.IDENTIFICATION))) {
			idfDS.add((Dataset) this);
		}

		if (children != null) {
			for (Dataset ds : children) {
				if (ds != null) {
					idfDS.addAll(ds.getIdentificationDatasets());
				}
			}
		}

		return idfDS;

	}

	public TransientData getTransientData() {
		if (transientData == null) {
			transientData = new TransientData();
		}
		return transientData;
	}

	/**
	 * Transient Data which will be not saved in database Used by the Proline Studio IHM
	 * 
	 * @author JM235353
	 */
	public static class TransientData implements Serializable {

		private static final long serialVersionUID = 1L;
		private ResultSummary resultSummary = null; // JPM.WART : uds package has no access to msi package
		private ResultSet resultSet = null;

		protected TransientData() {
		}

		public ResultSummary getResultSummary() {
			return resultSummary;
		}

		public void setResultSummary(ResultSummary resultSummary) {
			this.resultSummary = resultSummary;
		}

		public ResultSet getResultSet() {
			return resultSet;
		}

		public void setResultSet(ResultSet resultSet) {
			this.resultSet = resultSet;
		}
	}

}
