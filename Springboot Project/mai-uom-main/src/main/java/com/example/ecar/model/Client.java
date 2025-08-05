package com.example.ecar.model;

import java.util.Set;

import javax.persistence.Entity;
import javax.persistence.OneToMany;

@Entity
public class Client extends AuthenticatedUser {
	
	private String firstName;
	
	private String lastName;
	
	private String email;
	
	@OneToMany(mappedBy="tester")
	private Set<Reservation> reservations;

	public Client() {
	}

	public Client(int afm, String password, String firstName, String lastName, String email) {
		super(afm, password);
		this.setFirstName(firstName);
		this.setLastName(lastName);
		this.setEmail(email);
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public Set<Reservation> getReservations() {
		return reservations;
	}

	public void setReservations(Set<Reservation> reservations) {
		this.reservations = reservations;
	}
	
	public int getRole() {
		return 1;
	}



}
