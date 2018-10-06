package com.paypal.udc.service;

import java.util.List;
import com.paypal.udc.entity.User;
import com.paypal.udc.exception.ValidationError;


public interface IUserService {

    List<User> getAllUsers();

    User getUserById(long userId);

    User addUser(User user) throws ValidationError;

    User updateUser(User user) throws ValidationError;

    User deleteUser(long userId) throws ValidationError;

    User getUserByName(String userName);

    User enableUser(long userId) throws ValidationError;

}
