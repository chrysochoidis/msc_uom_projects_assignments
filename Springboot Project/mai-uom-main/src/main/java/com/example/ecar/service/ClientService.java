package com.example.ecar.service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.example.ecar.exception.AuthFailureException;
import com.example.ecar.exception.BadRequestException;
import com.example.ecar.exception.NotFoundException;
import com.example.ecar.model.Car;
import com.example.ecar.model.Client;
import com.example.ecar.model.Reservation;
import com.example.ecar.repository.CarRepository;
import com.example.ecar.repository.ClientRepository;
import com.example.ecar.repository.ReservationRepository;

@Service
public class ClientService {
	private static final int MAX_TIME_SLOTS_PER_DAY = 16;
	private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

	@Autowired
	private ClientRepository clientRepo;

	@Autowired
	private CarRepository carRepo;

	@Autowired
	private ReservationRepository reservationRepo;

	/**
	 * Retrieves all cars.
	 * 
	 * @return a list of cars
	 */
	public List<Car> getAllCars() {
		return carRepo.findAll();
	}

	/**
	 * Validates token and retrieves the client associated with it.
	 * 
	 * @param token the client token
	 * @return the client
	 */
	public Client getClientByToken(String token) {
		List<Client> clientList = clientRepo.findByToken(token);
		if (clientList.size() != 1) {
			throw new AuthFailureException();
		}
		return clientList.get(0);
	}

	/**
	 * Searches cars with one or more filters. Null parameters are not used in
	 * search.
	 * 
	 * @param brand      the car brand
	 * @param model      the car model
	 * @param fuel       the car fuel
	 * @param seats      the number of car seats
	 * @param fromPrice  the minimum acceptable price
	 * @param toPrice    the maximum acceptable price
	 * @param fromEngine the minimum acceptable engine
	 * @param toEngine   the maximum acceptable engine
	 * @return a list of cars matching the criteria
	 */
	public List<Car> searchCars(String brand, String model, String fuel, Integer seats, Double fromPrice,
			Double toPrice, Integer fromEngine, Integer toEngine) {
		return carRepo.search(brand, model, fuel, seats, fromPrice, toPrice, fromEngine, toEngine);
	}

	/**
	 * Purchases a car and decreases the available amount.
	 * 
	 * @param token the token of the client making the purchase
	 * @param id    the id of the car
	 */
	public void buyCar(String token, int id) {
		getClientByToken(token);
		if (carRepo.buyCar(id) != 1) {
			throw new BadRequestException("Could not complete transaction");
		}
	}

	/**
	 * Returns available time slots for test-drive of a car for a specific day.
	 * 
	 * Slots are represented by integers: eg. slot 0 is the first slot of that day
	 * (09:00 - 09:30), slot 1 is the second slot (09:30 - 10:00) etc.
	 * 
	 * Note: we assume there is only one available car of each kind for test-drives
	 * at a time.
	 * 
	 * @param token the token of the client requesting the test-drive
	 * @param carId the id of the car
	 * @param day   the day in yyyy-MM-dd format
	 * @return a list of integers representing the available slots
	 */
	public Set<Integer> getAvailableTimeSlotsForDay(String token, int carId, String day) {
		getClientByToken(token);
		Car car = carRepo.findById(carId).orElseThrow(NotFoundException::new);
		List<Integer> bookedSlots = reservationRepo.findSlotsByDate(car, parseDate(day));
		HashSet<Integer> availableSlots = new HashSet<Integer>();
		for (int i = 0; i < MAX_TIME_SLOTS_PER_DAY; i++) {
			availableSlots.add(i);
		}
		availableSlots.removeAll(bookedSlots);
		return availableSlots;
	}

	/**
	 * Makes a reservation for a test drive. This method is synchronized to avoid
	 * race conditions between checking for availability of the slot and actually
	 * making the reservation.
	 * 
	 * Slots are represented by integers: eg. slot 0 is the first slot of that day
	 * (09:00 - 09:30), slot 1 is the second slot (09:30 - 10:00) etc.
	 * 
	 * Note: we assume there is only one available car of each kind for test-drives
	 * at a time.
	 * 
	 * @param token    the token of the client making the reservation
	 * @param carId    the car id
	 * @param day      the day in yyyy-MM-dd format
	 * @param timeslot the specific slot for that day
	 */
	public synchronized void reserveCar(String token, int carId, String day, int timeslot) {
		Client tester = getClientByToken(token);
		Car car = carRepo.findById(carId).orElseThrow(NotFoundException::new);
		Date date = parseDate(day);
		if (timeslot >= MAX_TIME_SLOTS_PER_DAY || timeslot < 0) {
			throw new BadRequestException("Timeslot can only take values from 0 to " + (MAX_TIME_SLOTS_PER_DAY - 1));
		}
		if (!reservationRepo.findByCarAndTime(car, date, timeslot).isEmpty()) {
			throw new BadRequestException("Selected time slot is not available for this car");
		}
		reservationRepo.save(new Reservation(tester, date, timeslot, car));
	}

	/**
	 * Helper method to parse a string as Date or throw the proper exception if the
	 * string is not valid.
	 * 
	 * @param source a string of a date in yyyy-MM-dd format
	 * @return the Date object parsed from the source string
	 */
	private Date parseDate(String source) {
		try {
			return DATE_FORMATTER.parse(source);
		} catch (ParseException e) {
			throw new BadRequestException("Invalid date: " + source);
		}
	}

}
