package com.paypal.udc.dao;

import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.User;


public interface UserRepository extends CrudRepository<User, Long> {

    User findByUserName(String userName);
}
