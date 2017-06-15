package edu.csula.datascience.acquisition;

public class Crime {
	
	private String CID;
	private String CDate;
	private String Description;
	private String Street;
	private String City;
	private String location;
	private String ZipCode;
	
	public Crime(String CID,String cDate, String description, String street, String city, String geoLocation, String zipCode) {
		this.setCID(CID);
		this.CDate = cDate;
		this.Description = description;
		this.Street = street;
		this.City = city;
		this.location = geoLocation;
		this.ZipCode = zipCode;
	}

	public String getCDate() {
		return CDate;
	}

	public void setCDate(String cDate) {
		CDate = cDate;
	}

	public String getDescription() {
		return Description;
	}

	public void setDescription(String description) {
		Description = description;
	}

	public String getStreet() {
		return Street;
	}

	public void setStreet(String street) {
		Street = street;
	}

	public String getCity() {
		return City;
	}

	public void setCity(String city) {
		City = city;
	}

	public String getGeoLocation() {
		return location;
	}

	public void setGeoLocation(String geoLocation) {
		location = geoLocation;
	}

	public String getZipCode() {
		return ZipCode;
	}

	public void setZipCode(String zipCode) {
		ZipCode = zipCode;
	}

	public String getCID() {
		return CID;
	}

	public void setCID(String cID) {
		CID = cID;
	}
	
	
}
