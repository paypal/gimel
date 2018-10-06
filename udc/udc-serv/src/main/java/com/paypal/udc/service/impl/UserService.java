package com.paypal.udc.service.impl;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import com.paypal.udc.dao.UserRepository;
import com.paypal.udc.entity.User;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IUserService;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.user.UserFullNameValidator;
import com.paypal.udc.validator.user.UserNameValidator;


@Service
public class UserService implements IUserService {

    @Autowired
    private UserRepository userRepository;
    @Autowired
    private UserFullNameValidator s2;
    @Autowired
    private UserNameValidator s1;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
    final static Logger logger = LoggerFactory.getLogger(UserService.class);

    @Override
    public User getUserById(final long userId) {
        return this.userRepository.findOne(userId);
    }

    @Override
    public List<User> getAllUsers() {
        final List<User> users = new ArrayList<User>();
        this.userRepository.findAll().forEach(user -> {
            users.add(user);
        });
        return users;
    }

    @Override
    public User deleteUser(final long userId) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        User user = this.userRepository.findOne(userId);
        if (user != null) {
            user.setUpdatedTimestamp(sdf.format(timestamp));
            user.setIsActiveYN(ActiveEnumeration.NO.getFlag());
            user = this.userRepository.save(user);
            return user;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("User ID is invalid");
            throw v;
        }
    }

    @Override
    public User addUser(final User user) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        try {
            user.setUpdatedUser(user.getCreatedUser());
            user.setCreatedTimestamp(sdf.format(timestamp));
            user.setUpdatedTimestamp(sdf.format(timestamp));
            user.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            final User insertedUser = this.userRepository.save(user);
            return insertedUser;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("User name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("User name is duplicated");
            throw v;
        }
    }

    @Override
    public User updateUser(final User user) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        User tempUser = this.userRepository.findOne(user.getUserId());
        if (tempUser != null) {
            try {
                tempUser.setUpdatedUser(user.getCreatedUser());
                tempUser.setUpdatedTimestamp(sdf.format(timestamp));
                this.s1.setNextChain(this.s2);
                this.s1.validate(user, tempUser);
                tempUser = this.userRepository.save(tempUser);
                return tempUser;
            }
            catch (final TransactionSystemException e) {
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("User name is empty");
                throw v;
            }
            catch (final DataIntegrityViolationException e) {
                v.setErrorCode(HttpStatus.CONFLICT);
                v.setErrorDescription("User name is duplicated");
                throw v;
            }
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("User ID is invalid");
            throw v;
        }
    }

    @Override
    public User getUserByName(final String userName) {
        return this.userRepository.findByUserName(userName);
    }

    @Override
    public User enableUser(final long userId) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        User user = this.userRepository.findOne(userId);
        if (user != null) {
            user.setUpdatedTimestamp(sdf.format(timestamp));
            user.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            user = this.userRepository.save(user);
            return user;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("User ID is invalid");
            throw v;
        }
    }
}
