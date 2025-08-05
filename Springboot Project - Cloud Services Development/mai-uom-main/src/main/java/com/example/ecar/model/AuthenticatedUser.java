package com.example.ecar.model;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;

@Entity
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
public abstract class AuthenticatedUser {
	
	@Id
	protected int afm;
	
	protected String password;
	
	protected String token;
	
	public AuthenticatedUser() {
	}
	
	public AuthenticatedUser(int afm, String password) {
		this.afm = afm;
		this.password = password;
	}

	public int getAfm() {
		return afm;
	}

	public void setAfm(int afm) {
		this.afm = afm;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}
	
	public abstract int getRole();

}
