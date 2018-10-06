package com.paypal.udc.validator.user;

import org.springframework.stereotype.Component;
import com.paypal.udc.entity.User;
import com.paypal.udc.exception.ValidationError;


@Component
public class UserNameValidator implements UserValidator {

    private UserValidator chain;

    @Override
    public void setNextChain(final UserValidator nextChain) {
        this.chain = nextChain;
    }

    @Override
    public void validate(final User user, final User updatedUser) throws ValidationError {
        if (user.getUserName() != null && user.getUserName().length() >= 0) {
            updatedUser.setUserName(user.getUserName());
        }
        this.chain.validate(user, updatedUser);
    }

}
