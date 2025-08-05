package com.example.ecar.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.example.ecar.exception.AuthFailureException;
import com.example.ecar.exception.NotFoundException;
import com.example.ecar.model.Car;
import com.example.ecar.model.Dealership;
import com.example.ecar.repository.CarRepository;
import com.example.ecar.repository.DealershipRepository;

@Service
public class DealershipService {

	@Autowired
	private CarRepository carRepo;

	@Autowired
	private DealershipRepository dealershipRepo;

	/**
	 * Validates token and retrieves the dealership associated with it.
	 * 
	 * @param token the dealership token
	 * @return the dealership
	 */
	public Dealership getDealershipByToken(String token) {
		List<Dealership> dealerList = dealershipRepo.findByToken(token);
		if (dealerList.size() != 1) {
			throw new AuthFailureException();
		}
		return dealerList.get(0);
	}

	/**
	 * Retrieves the cars that belong to this dealership.
	 * 
	 * @param token the token of the dealership
	 * @return a list of cars belonging to the dealership
	 */
	public List<Car> getCars(String token) {
		Dealership d = getDealershipByToken(token);
		return d.getCars();
	}

	/**
	 * Adds a new car to the dealership.
	 * 
	 * @param token  the token of the dealership
	 * @param brand  the car brand
	 * @param model  the car model
	 * @param fuel   the car fuel
	 * @param engine the car engine
	 * @param seats  the number of the seats
	 * @param price  the car price
	 * @param info   additional information about the car
	 * @param amount the number of cars available for purchase
	 */
	public void addCar(String token, String brand, String model, String fuel, int engine, int seats, double price,
			String info, int amount) {
		Dealership d = getDealershipByToken(token);
		Car newCar = new Car(brand, model, fuel, engine, seats, price, info, amount);
		newCar.setDealership(d);
		carRepo.save(newCar);
	}

	/**
	 * Update the information of a car.
	 * 
	 * @param token        the token of the dealership
	 * @param newCarValues an object holding the updated values
	 */
	public void updateCar(String token, Car newCarValues) {
		Dealership d = getDealershipByToken(token);
		Car c = carRepo.findByIdAndDealership(newCarValues.getId(), d).orElseThrow(NotFoundException::new);
		c.setBrand(newCarValues.getBrand());
		c.setModel(newCarValues.getModel());
		c.setFuel(newCarValues.getFuel());
		c.setEngine(newCarValues.getEngine());
		c.setSeats(newCarValues.getSeats());
		c.setPrice(newCarValues.getPrice());
		c.setInfo(newCarValues.getInfo());
		c.setAmount(newCarValues.getAmount());
		carRepo.save(c);
	}

}
