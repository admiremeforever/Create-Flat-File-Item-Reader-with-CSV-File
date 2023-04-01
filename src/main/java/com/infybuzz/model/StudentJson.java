package com.infybuzz.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

//to skip fields which are not required
@JsonIgnoreProperties(ignoreUnknown = true)
public class StudentJson {

	private Long id;
   //to map fields to json even if the field name in POJO is diffrent form that in json
	@JsonProperty("first_Name")
	private String firstName;

	//private String lastName;

	private String email;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

//	public String getLastName() {
//		return lastName;
//	}
//
//	public void setLastName(String lastName) {
//		this.lastName = lastName;
//	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

//	@Override
//	public String toString() {
//		return "StudentCsv [id=" + id + ", firstName=" + firstName + ", lastName=" + lastName + ", email=" + email
//				+ "]";
//	}
	
	@Override
	public String toString() {
		return "StudentCsv [id=" + id + ", firstName=" + firstName +  ", email=" + email
				+ "]";
	}

}
