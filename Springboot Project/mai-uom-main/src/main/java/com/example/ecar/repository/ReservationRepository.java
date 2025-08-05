package com.example.ecar.repository;

import java.util.Date;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.example.ecar.model.Car;
import com.example.ecar.model.Reservation;

public interface ReservationRepository extends JpaRepository<Reservation, Integer> {
	
	@Query(
		"SELECT r FROM Reservation r " + 
		"WHERE r.reservationDay = :day " + 
		"AND r.timeslot = :slot " +
		"AND r.car = :c ")
	List<Reservation> findByCarAndTime(Car c, Date day, int slot);
	
	@Query(
		"SELECT r.timeslot FROM Reservation r " + 
		"WHERE r.reservationDay = :day " +
		"AND r.car = :c ")
	List<Integer> findSlotsByDate(Car c, Date day);

}
