package com.example.ecar.controller;

import java.util.List;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.ecar.dto.AddCarRequestDto;
import com.example.ecar.model.Car;
import com.example.ecar.service.DealershipService;

@CrossOrigin(origins = "*", allowedHeaders = "*")
@RequestMapping("/dealership")
@RestController
public class DealershipController {

	@Autowired
	private DealershipService dealershipService;

	@GetMapping("/getCars")
	public List<Car> getCars(@RequestParam String token) {
		return dealershipService.getCars(token);
	}

	@PostMapping("/addCar")	
	public String addCar(@RequestParam String token, @Valid @RequestBody AddCarRequestDto dto) {
		dealershipService.addCar(token, dto.brand, dto.model, dto.fuel, dto.engine, dto.seats, dto.price, dto.info, dto.amount);
		return "Το αυτοκίνητο προστέθηκε!";
	}

	@PostMapping("/updateCar")
	public void updateCar(@RequestParam String token, @RequestBody Car c) {
		dealershipService.updateCar(token, c);
	}

}
