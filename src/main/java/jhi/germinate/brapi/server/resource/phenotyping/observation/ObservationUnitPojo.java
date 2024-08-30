package jhi.germinate.brapi.server.resource.phenotyping.observation;

import java.sql.Timestamp;
import java.util.List;

public class ObservationUnitPojo
{
	private String observationUnitDbId;
	private String germplasmDbId;
	private String germplasmName;
	private String trialRow;
	private String trialColumn;
	private String rep;
	private String studyDbId;
	private List<ObservationUnitData> unitData;

	public String getObservationUnitDbId()
	{
		return observationUnitDbId;
	}

	public ObservationUnitPojo setObservationUnitDbId(String observationUnitDbId)
	{
		this.observationUnitDbId = observationUnitDbId;
		return this;
	}

	public String getGermplasmDbId()
	{
		return germplasmDbId;
	}

	public ObservationUnitPojo setGermplasmDbId(String germplasmDbId)
	{
		this.germplasmDbId = germplasmDbId;
		return this;
	}

	public String getGermplasmName()
	{
		return germplasmName;
	}

	public ObservationUnitPojo setGermplasmName(String germplasmName)
	{
		this.germplasmName = germplasmName;
		return this;
	}

	public String getTrialRow()
	{
		return trialRow;
	}

	public ObservationUnitPojo setTrialRow(String trialRow)
	{
		this.trialRow = trialRow;
		return this;
	}

	public String getTrialColumn()
	{
		return trialColumn;
	}

	public ObservationUnitPojo setTrialColumn(String trialColumn)
	{
		this.trialColumn = trialColumn;
		return this;
	}

	public String getRep()
	{
		return rep;
	}

	public ObservationUnitPojo setRep(String rep)
	{
		this.rep = rep;
		return this;
	}

	public String getStudyDbId()
	{
		return studyDbId;
	}

	public ObservationUnitPojo setStudyDbId(String studyDbId)
	{
		this.studyDbId = studyDbId;
		return this;
	}

	public List<ObservationUnitData> getUnitData()
	{
		return unitData;
	}

	public ObservationUnitPojo setUnitData(List<ObservationUnitData> unitData)
	{
		this.unitData = unitData;
		return this;
	}

	public static class ObservationUnitData
	{
		private String    observationVariableDbId;
		private String    observationVariableName;
		private String    observationUnitDbId;
		private Timestamp observationTimeStamp;
		private String    value;
		private Double    latitude;
		private Double    longitude;
		private Double    elevation;

		public String getObservationVariableDbId()
		{
			return observationVariableDbId;
		}

		public ObservationUnitData setObservationVariableDbId(String observationVariableDbId)
		{
			this.observationVariableDbId = observationVariableDbId;
			return this;
		}

		public String getObservationVariableName()
		{
			return observationVariableName;
		}

		public ObservationUnitData setObservationVariableName(String observationVariableName)
		{
			this.observationVariableName = observationVariableName;
			return this;
		}

		public String getObservationUnitDbId()
		{
			return observationUnitDbId;
		}

		public ObservationUnitData setObservationUnitDbId(String observationUnitDbId)
		{
			this.observationUnitDbId = observationUnitDbId;
			return this;
		}

		public Timestamp getObservationTimeStamp()
		{
			return observationTimeStamp;
		}

		public ObservationUnitData setObservationTimeStamp(Timestamp observationTimeStamp)
		{
			this.observationTimeStamp = observationTimeStamp;
			return this;
		}

		public String getValue()
		{
			return value;
		}

		public ObservationUnitData setValue(String value)
		{
			this.value = value;
			return this;
		}

		public Double getLatitude()
		{
			return latitude;
		}

		public ObservationUnitData setLatitude(Double latitude)
		{
			this.latitude = latitude;
			return this;
		}

		public Double getLongitude()
		{
			return longitude;
		}

		public ObservationUnitData setLongitude(Double longitude)
		{
			this.longitude = longitude;
			return this;
		}

		public Double getElevation()
		{
			return elevation;
		}

		public ObservationUnitData setElevation(Double elevation)
		{
			this.elevation = elevation;
			return this;
		}
	}
}
