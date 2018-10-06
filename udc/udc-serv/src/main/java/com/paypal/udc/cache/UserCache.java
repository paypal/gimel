package com.paypal.udc.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import com.paypal.udc.dao.UserRepository;
import com.paypal.udc.entity.User;


@Component
public class UserCache {

    final static Logger logger = LoggerFactory.getLogger(UserCache.class);

    @Autowired
    UserRepository userRepository;

    @Cacheable(value = "userCache", key = "#userId")
    public User getUser(final Long userId) {
        return this.userRepository.findOne(userId);
    }
}
