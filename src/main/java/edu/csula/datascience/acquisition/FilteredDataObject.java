package edu.csula.datascience.acquisition;

public class FilteredDataObject {

	private String ID;
	private String Date;
	private String Street;
	private String City;
	private String ZipCode;
	private String Location;
	private String Source;
	
	public String getID() {
		return ID;
	}

	public void setID(String iD) {
		ID = iD;
	}

	public String getDate() {
		return Date;
	}

	public void setDate(String date) {
		Date = date;
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

	public String getZipCode() {
		return ZipCode;
	}

	public void setZipCode(String zipCode) {
		ZipCode = zipCode;
	}

	public String getLocation() {
		return Location;
	}

	public void setLocation(String location) {
		Location = location;
	}

	public String getSource() {
		return Source;
	}

	public void setSource(String source) {
		Source = source;
	}

	public FilteredDataObject(String iD, String date, String street, String city, String zipCode, String location,
			String source) {
		super();
		ID = iD;
		Date = date;
		Street = street;
		City = city;
		ZipCode = zipCode;
		Location = location;
		Source = source;
	}
	
}
