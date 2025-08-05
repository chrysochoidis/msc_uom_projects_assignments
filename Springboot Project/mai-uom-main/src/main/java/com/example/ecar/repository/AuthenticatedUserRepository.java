package com.example.ecar.repository;

import java.util.Optional;

import javax.transaction.Transactional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import com.example.ecar.model.AuthenticatedUser;

public interface AuthenticatedUserRepository extends JpaRepository<AuthenticatedUser, Integer> {
	
	@Query("SELECT u FROM AuthenticatedUser u WHERE u.afm = :afm AND u.password = :password")
	Optional<AuthenticatedUser> findByAfmAndPassword(int afm, String password);
	
	@Modifying
	@Transactional
	@Query("UPDATE AuthenticatedUser u SET u.token = '' WHERE u.token = :token")
	int logout(String token);
}
