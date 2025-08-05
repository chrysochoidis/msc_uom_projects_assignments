package com.example.ecar.exception;

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

/**
 * Global exception handler for all controllers, handling our custom exceptions
 * and returning the proper HTTP status code with an informative message.
 */
@ControllerAdvice
public class GlobalExceptionHandler {

	@ExceptionHandler(AuthFailureException.class)
	public ResponseEntity<String> handleAuthFailure(AuthFailureException ex) {
		return ResponseEntity.status(401).body("Λάθος Διαπιστευτήρια");
	}

	@ExceptionHandler(NotFoundException.class)
	public ResponseEntity<String> handleNotFound(NotFoundException ex) {
		return ResponseEntity.status(404).body("Resource not found");
	}

	@ExceptionHandler(AlreadyExistsException.class)
	public ResponseEntity<String> handleAlreadyExists(AlreadyExistsException ex) {
		return ResponseEntity.status(409).body("Υπάρχει ήδη λογαριασμός με αυτόν τον ΑΦΜ");
	}

	@ExceptionHandler(BadRequestException.class)
	public ResponseEntity<String> handleBadRequest(BadRequestException ex) {
		return ResponseEntity.status(400).body(ex.getMessage());
	}

	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<Map<String, String>> handleValidationExceptions(MethodArgumentNotValidException ex) {
		Map<String, String> errors = new HashMap<>();
		for (FieldError error : ex.getBindingResult().getFieldErrors()) {
			errors.put(error.getField(), error.getDefaultMessage());
		}
		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errors);
	}
}
