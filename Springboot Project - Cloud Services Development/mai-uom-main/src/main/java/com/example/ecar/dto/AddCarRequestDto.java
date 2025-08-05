package com.example.ecar.dto;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

public class AddCarRequestDto {
	@NotBlank(message="Η μάρκα είναι υποχρεωτική")
	public String brand;
	
	@NotBlank(message="Το μοντέλο είναι υποχρεωτικό")
	public String model;
	
	@NotBlank(message="Το είδος καυσίμου είναι υποχρεωτικό")
	public String fuel;
	
	@Min(value=500, message="Τα cc μηχανής είναι υποχρεωτικά με ελάχιστη τιμή 500")
	public int engine;
	
	@Min(value=1, message="Ο αριθμός θέσεων είναι υποχρεωτικός με ελάχιστη τιμή 1")
	public int seats;
	
	@Min(value=1, message="Η τιμή είναι υποχρεωτική με ελάχιστη τιμή 1")
	public double price;
	
	public String info;
	
	@Min(value=1, message="Ο αριθμός διαθεσιμότητας είναι υποχρεωτικός με ελάχιστη τιμή 1")
	@Max(value=20000, message="Ο αριθμός διαθεσιμότητας είναι υποχρεωτικός με μέγιστη τιμή 20000")
	public int amount;
}
