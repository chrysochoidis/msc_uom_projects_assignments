package com.example.ecar.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.ecar.dto.ClientRegisterRequestDto;
import com.example.ecar.dto.DealershipRegisterRequestDto;
import com.example.ecar.dto.LoginRequestDto;
import com.example.ecar.dto.LoginResponseDto;
import com.example.ecar.service.UserService;

@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
public class UserController {

	@Autowired
	private UserService userService;

	@PostMapping("/login")
	public LoginResponseDto login(@RequestBody LoginRequestDto dto) {
		return userService.login(dto.afm, dto.password);
	}
	
	@GetMapping("/logout")
	public void logout(@RequestParam String token) {
		userService.logout(token);
	}

	@PostMapping("/clientRegister")
	public String clientRegister(@Valid @RequestBody ClientRegisterRequestDto dto) {
		userService.clientRegister(dto.afm, dto.password, dto.firstName, dto.lastName, dto.email);
		return "Ο χρήστης δημιουργήθηκε";
	}

	@PostMapping("/dealershipRegister")
	public String dealershipRegister(@Valid @RequestBody DealershipRegisterRequestDto dto) {
		userService.dealershipRegister(dto.afm, dto.password, dto.name, dto.owner);
		return "Ο χρήστης δημιουργήθηκε";
	}

}
