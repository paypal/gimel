package com.paypal.udc.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.paypal.udc.cache.UserCache;
import com.paypal.udc.dao.UserRepository;
import com.paypal.udc.entity.User;
import com.paypal.udc.exception.ValidationError;


@Component
public class UserUtil {

    @Autowired
    private UserCache userCache;
    @Autowired
    private UserRepository userRepository;

    public void validateUser(final long userId) throws ValidationError {

        final User user = this.userCache.getUser(userId);
        if (user == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Approver ID is not valid");
            throw v;
        }
    }

    public User validateUser(final String userName) throws ValidationError {
        if (userName == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Username is not supplied");
            throw v;
        }
        final User user = this.userRepository.findByUserName(userName);
        if (user == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Username is not valid");
            throw v;
        }
        return user;
    }

}
