package com.example.ecar.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.example.ecar.model.Dealership;

public interface DealershipRepository extends JpaRepository<Dealership, Integer> {

	@Query("SELECT d FROM Dealership d WHERE d.token = :token")
	List<Dealership> findByToken(String token);

}
