package com.example.ecar.dto;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

public class DealershipRegisterRequestDto {
	@Min(value=100000000, message="Ο ΑΦΜ πρέπει να είναι 9ψήφιος")
	@Max(value=999999999, message="Ο ΑΦΜ πρέπει να είναι 9ψήφιος")
	public int afm;
	
	@NotBlank(message="Το password είναι υποχρεωτικό")
	public String password;
	
	@NotBlank(message="Η επωνυμία είναι υποχρεωτική")
	public String name;
	
	@NotBlank(message="Το όνομα ιδιοκτήτη είναι υποχρεωτικό")
	public String owner;
}
