package com.paypal.udc.controller;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.google.gson.Gson;
import com.paypal.udc.cache.UserCache;
import com.paypal.udc.dao.UserRepository;
import com.paypal.udc.entity.User;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IUserService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("user")
@Api(value = "UserService", description = "Operations pertaining to User")
public class UserController {

    final static Logger logger = LoggerFactory.getLogger(UserController.class);
    final Gson gson = new Gson();
    private IUserService userService;
    private UserCache userCache;
    private UserRepository userRepository;
    @Value("${application.env}")
    private String isProd;

    @Autowired
    private UserController(final IUserService userService, final UserCache userCache,
            final UserRepository userRepository) {

        this.userCache = userCache;
        this.userRepository = userRepository;
        this.userService = userService;
    }

    @ApiOperation(value = "View the User based on Name", response = User.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved User"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("userByName/{name:.+}")
    public ResponseEntity<User> getUserByName(@PathVariable("name") final String name) {
        final User user = this.userRepository.findByUserName(name);
        return new ResponseEntity<User>(user, HttpStatus.OK);
    }

    @ApiOperation(value = "View the User based on ID", response = User.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved User"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("user/{id}")
    public ResponseEntity<User> getUserById(@PathVariable("id") final Long id) {
        final User user = this.userCache.getUser(id);
        return new ResponseEntity<User>(user, HttpStatus.OK);
    }

    @ApiOperation(value = "View a list of available Users", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("users")
    public ResponseEntity<List<User>> getAllUsers() {
        final List<User> list = this.userService.getAllUsers();
        return new ResponseEntity<List<User>>(list, HttpStatus.OK);
    }

    @ApiOperation(value = "Insert an User", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted User"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("user")
    public ResponseEntity<String> addUser(@RequestBody final User user) {
        User insertedUser;
        try {
            insertedUser = this.userService.addUser(user);
            return new ResponseEntity<String>(this.gson.toJson(insertedUser), HttpStatus.CREATED);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Update an user based on Input", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated User"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("user")
    public ResponseEntity<String> updateUser(@RequestBody final User user) {
        User updatedUser;
        try {
            updatedUser = this.userService.updateUser(user);
            return new ResponseEntity<String>(this.gson.toJson(updatedUser), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Delete an user based on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deleted User"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("duser/{id}")
    public ResponseEntity<String> deleteUser(@PathVariable("id") final Integer id) {
        try {
            final User user = this.userService.deleteUser(id);
            return new ResponseEntity<String>(this.gson.toJson("Deleted " + user.getUserId()), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }

    }

    @ApiOperation(value = "Activate an user based on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully activated User"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("euser/{id}")
    public ResponseEntity<String> activateUser(@PathVariable("id") final Integer id) {
        try {
            final User user = this.userService.enableUser(id);
            return new ResponseEntity<String>(this.gson.toJson("Enabled " + user.getUserId()), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }
}
