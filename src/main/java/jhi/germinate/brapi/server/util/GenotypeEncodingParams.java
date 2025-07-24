package jhi.germinate.brapi.server.util;

public class GenotypeEncodingParams
{
	private boolean collapse      = true;
	private String  unknownString = "N";
	private String  sepPhased     = "|";
	private String  sepUnphased   = "/";

	public GenotypeEncodingParams()
	{
	}

	public GenotypeEncodingParams(boolean collapse, String unknownString, String sepPhased, String sepUnphased)
	{
		this.collapse = collapse;
		this.unknownString = unknownString;
		this.sepPhased = sepPhased;
		this.sepUnphased = sepUnphased;
	}

	public boolean isCollapse()
	{
		return collapse;
	}

	public GenotypeEncodingParams setCollapse(boolean collapse)
	{
		this.collapse = collapse;
		return this;
	}

	public String getUnknownString()
	{
		return unknownString;
	}

	public GenotypeEncodingParams setUnknownString(String unknownString)
	{
		if (unknownString != null)
			this.unknownString = unknownString;
		return this;
	}

	public String getSepPhased()
	{
		return sepPhased;
	}

	public GenotypeEncodingParams setSepPhased(String sepPhased)
	{
		if (sepPhased != null)
			this.sepPhased = sepPhased;
		return this;
	}

	public String getSepUnphased()
	{
		return sepUnphased;
	}

	public GenotypeEncodingParams setSepUnphased(String sepUnphased)
	{
		if (sepUnphased != null)
			this.sepUnphased = sepUnphased;
		return this;
	}

	@Override
	public String toString()
	{
		return "GenotypeEncodingParams{" +
			"collapse=" + collapse +
			", unknownString='" + unknownString + '\'' +
			", sepPhased='" + sepPhased + '\'' +
			", sepUnphased='" + sepUnphased + '\'' +
			'}';
	}
}