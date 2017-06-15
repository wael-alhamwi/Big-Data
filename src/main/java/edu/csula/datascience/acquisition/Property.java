package edu.csula.datascience.acquisition;

public class Property {

	private String PID;
	private String RecordingDate;
	private String StreetName;
	private String CityName;
	private String ZipCode;
	private String UnitNo;
	private String location;
	
	public Property(String PID, String recordingDate, String streetName, String cityName, String zipCode, String unitNo,
			String geoLocation) {
		this.setPID(PID);
		this.RecordingDate = recordingDate;
		this.StreetName = streetName;
		this.CityName = cityName;
		this.ZipCode = zipCode;
		this.UnitNo = unitNo;
		this.location = geoLocation;
	}

	public String getRecordingDate() {
		return RecordingDate;
	}

	public void setRecordingDate(String recordingDate) {
		RecordingDate = recordingDate;
	}

	public String getStreetName() {
		return StreetName;
	}

	public void setStreetName(String streetName) {
		StreetName = streetName;
	}

	public String getCityName() {
		return CityName;
	}

	public void setCityName(String cityName) {
		CityName = cityName;
	}

	public String getZipCode() {
		return ZipCode;
	}

	public void setZipCode(String zipCode) {
		ZipCode = zipCode;
	}

	public String getUnitNo() {
		return UnitNo;
	}

	public void setUnitNo(String unitNo) {
		UnitNo = unitNo;
	}

	public String getGeoLocation() {
		return location;
	}

	public void setGeoLocation(String geoLocation) {
		location = geoLocation;
	}

	public String getPID() {
		return PID;
	}

	public void setPID(String pID) {
		PID = pID;
	}
		
	
}
