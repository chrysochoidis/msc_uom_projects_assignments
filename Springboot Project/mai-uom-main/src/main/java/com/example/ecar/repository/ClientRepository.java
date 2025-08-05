package com.example.ecar.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.example.ecar.model.Client;

public interface ClientRepository extends JpaRepository<Client, Integer> {

	@Query("SELECT c FROM Client c WHERE c.token = :token")
	List<Client> findByToken(String token);
}
