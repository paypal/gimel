package com.paypal.udc.validator.user;

import com.paypal.udc.entity.User;
import com.paypal.udc.exception.ValidationError;


public interface UserValidator {

    void setNextChain(UserValidator nextChain);

    void validate(User user, final User updatedUser) throws ValidationError;
}
