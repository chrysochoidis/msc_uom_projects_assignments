package com.example.ecar.service;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.example.ecar.dto.LoginResponseDto;
import com.example.ecar.exception.AlreadyExistsException;
import com.example.ecar.exception.AuthFailureException;
import com.example.ecar.model.AuthenticatedUser;
import com.example.ecar.model.Client;
import com.example.ecar.model.Dealership;
import com.example.ecar.repository.AuthenticatedUserRepository;
import com.example.ecar.repository.ClientRepository;
import com.example.ecar.repository.DealershipRepository;

@Service
public class UserService {

	@Autowired
	private ClientRepository clientRepo;

	@Autowired
	private AuthenticatedUserRepository authUserRepo;

	@Autowired
	private DealershipRepository dealershipRepo;

	/**
	 * Validates afm/password of a user and generates a token to be used for
	 * authentication in further API calls. Also returns an integer representing the
	 * user role: 1 for clients and 2 for dealerships
	 * 
	 * @param afm      the afm of the user
	 * @param password the password of the user
	 * @return the generated token and the role of the user
	 */
	public LoginResponseDto login(int afm, String password) {
		LoginResponseDto result = new LoginResponseDto();
		AuthenticatedUser user = authUserRepo.findByAfmAndPassword(afm, password)
				.orElseThrow(AuthFailureException::new);

		result.token = UUID.randomUUID().toString();
		result.role = user.getRole();

		user.setToken(result.token);
		authUserRepo.save(user);

		return result;
	}

	/**
	 * Invalidates the given token.
	 * 
	 * @param token the token to invalidate
	 */
	public void logout(String token) {
		authUserRepo.logout(token);
	}

	/**
	 * Registers a new client after checking that there is not already a user with
	 * that afm.
	 * 
	 * @param afm       the afm, unique for each user
	 * @param password  the client's password
	 * @param firstName the client's first name
	 * @param lastName  the client's last name
	 * @param email     the client's email
	 */
	public void clientRegister(int afm, String password, String firstName, String lastName, String email) {
		if (authUserRepo.existsById(afm)) {
			throw new AlreadyExistsException();
		}
		clientRepo.save(new Client(afm, password, firstName, lastName, email));
	}

	/**
	 * Registers a new dealership after checking that there is not already a user
	 * with that afm
	 * 
	 * @param afm      the afm, unique for each user
	 * @param password the dealership's password
	 * @param name     the dealership's name
	 * @param owner    the name of the dealership's owner
	 */
	public void dealershipRegister(int afm, String password, String name, String owner) {
		if (authUserRepo.existsById(afm)) {
			throw new AlreadyExistsException();
		}
		dealershipRepo.save(new Dealership(afm, password, name, owner));
	}

}
