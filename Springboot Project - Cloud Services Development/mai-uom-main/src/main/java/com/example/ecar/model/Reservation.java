package com.example.ecar.model;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@Entity
public class Reservation {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int id;

	@ManyToOne
	private Client tester;
	
	@Temporal(TemporalType.DATE)
    private Date reservationDay;
	
	private int timeslot;
	
	@ManyToOne
	private Car car;
	
	public Reservation() {
	}
	
	public Reservation(Client tester, Date reservationDay, int timeslot, Car car) {
		this.tester = tester;
		this.reservationDay = reservationDay;
		this.timeslot = timeslot;
		this.car = car;
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public Client getTester() {
		return tester;
	}

	public void setTester(Client tester) {
		this.tester = tester;
	}

	public Date getReservationDay() {
		return reservationDay;
	}

	public void setReservationDay(Date reservationDay) {
		this.reservationDay = reservationDay;
	}

	public int getTimeslot() {
		return timeslot;
	}

	public void setTimeslot(int timeslot) {
		this.timeslot = timeslot;
	}

	public Car getCar() {
		return car;
	}

	public void setCar(Car car) {
		this.car = car;
	}

}
