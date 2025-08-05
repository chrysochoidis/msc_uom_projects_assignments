package com.example.ecar.model;

import java.util.Set;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;

@Entity
public class Car {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int id;
	
	private String brand;
	private String model;
	private String fuel;
	private int engine;
	private int seats;
	private double price;
	private String info;
	private int amount;
	
	@OneToMany(mappedBy="car")
	private Set<Reservation> reservations;
	
	@ManyToOne
	private Dealership dealership;

	public Car() {
	}

	public Car(String brand, String model, String fuel, int engine, int seats, double price, String info, int amount) {
		this.setBrand(brand);
		this.setModel(model);
		this.setFuel(fuel);
		this.setEngine(engine);
		this.setSeats(seats);
		this.setPrice(price);
		this.setInfo(info);
		this.setAmount(amount);
	}

	public void setDealership(Dealership dealership) {
		this.dealership = dealership;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public String getModel() {
		return model;
	}

	public void setModel(String model) {
		this.model = model;
	}

	public String getFuel() {
		return fuel;
	}

	public void setFuel(String fuel) {
		this.fuel = fuel;
	}

	public int getEngine() {
		return engine;
	}

	public void setEngine(int engine) {
		this.engine = engine;
	}

	public int getSeats() {
		return seats;
	}

	public void setSeats(int seats) {
		this.seats = seats;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

}
