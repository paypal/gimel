package com.paypal.udc.validator.user;

import org.springframework.stereotype.Component;
import com.paypal.udc.entity.User;
import com.paypal.udc.exception.ValidationError;


@Component
public class UserFullNameValidator implements UserValidator {

    private UserValidator chain;

    @Override
    public void setNextChain(final UserValidator nextChain) {
        this.chain = nextChain;
    }

    @Override
    public void validate(final User user, final User updatedUser) throws ValidationError {
        if (user.getUserFullName() != null && user.getUserFullName().length() >= 0) {
            updatedUser.setUserFullName(user.getUserFullName());
        }
    }

}
