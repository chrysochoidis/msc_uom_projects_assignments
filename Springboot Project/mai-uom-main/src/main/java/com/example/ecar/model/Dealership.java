package com.example.ecar.model;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.OneToMany;

@Entity
public class Dealership extends AuthenticatedUser {

	private String name;
	
	private String owner;

	@OneToMany(mappedBy = "dealership", cascade = CascadeType.ALL)
	private List<Car> cars = new ArrayList<Car>();

	public Dealership() {
	}

	public Dealership(int afm, String password, String name, String owner) {
		super(afm, password);
		this.setName(name);
		this.setOwner(owner);
	}
	
	public void addCar(Car car) {
		cars.add(car);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public List<Car> getCars() {
		return cars;
	}

	public void setCars(List<Car> cars) {
		this.cars = cars;
	}
	
	public int getRole() {
		return 2;
	}
	

}
