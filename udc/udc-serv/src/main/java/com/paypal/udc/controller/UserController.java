/*
 * Copyright 2019 PayPal Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.paypal.udc.controller;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.servlet.http.HttpServletRequest;
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
import com.paypal.udc.entity.user.User;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IUserService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
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
    private final IUserService userService;
    private final HttpServletRequest request;
    private final String username = "udcdev";
    private final String fullname = "UDC Developer";
    private final String role = "UDC_ADMIN_ROLE";
    private final String managername = "UDC Admin";
    private final String organization = "Mycorp";
    private final String qid = "QID";
    private final String location = "location";
    @Value("${application.env}")
    private String isProd;
    private final String userType;

    @Autowired
    private UserController(final IUserService userService,
            final HttpServletRequest request) {

        this.userService = userService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();

    }

    @ApiOperation(value = "View the User based on Name", response = User.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved User"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("userByName/{name:.+}")
    public ResponseEntity<?> getUserByName(@PathVariable("name") final String name) {
		final User user = this.userService.getUserByName(name);
		return new ResponseEntity<User>(user, HttpStatus.OK);
	}

    @ApiOperation(value = "View the User based on ID", response = User.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved User"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("user/{id}")
    public ResponseEntity<?> getUserById(@PathVariable("id") final Long id) {
		User user;
		try {
			user = this.userService.getUserById(id);
			return new ResponseEntity<User>(user, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}
    }

    @ApiOperation(value = "View the User based on HttpHeader", response = User.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved User"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("userDetails")
    public ResponseEntity<?> getUserFromSession() {

        User user;
        String userName = this.username;
        String fullName = this.fullname;
        String role = this.role;
        String managerName = this.managername;
        String organization = this.organization;
        String qid = this.qid;
        String location = this.location;
        if (this.isProd.equals("true") && this.request.getScheme().equals("https")) {

            final String userType = this.request.getAttribute("userType") == null ? this.userType
                    : this.request.getAttribute("userType").toString();

            if (userType.equals(UserAttributeEnumeration.SUCCESS.getFlag())) {
                userName = this.request.getAttribute("username").toString();
                fullName = this.request.getAttribute("fullname").toString();
                role = this.request.getAttribute("role") == null ? ""
                        : this.request.getAttribute("role").toString();
                managerName = this.request.getAttribute("manager") == null ? ""
                        : this.request.getAttribute("manager").toString();
                organization = this.request.getAttribute("organization") == null ? ""
                        : this.request.getAttribute("organization").toString();
                qid = this.request.getAttribute("qid") == null ? ""
                        : this.request.getAttribute("qid").toString();
                location = this.request.getAttribute("location") == null ? ""
                        : this.request.getAttribute("location").toString();

            }
            else if (userType.equals(UserAttributeEnumeration.FILE_MISSING.getFlag())) {
                final ValidationError verror = new ValidationError();
                verror.setErrorCode(HttpStatus.INTERNAL_SERVER_ERROR);
                verror.setErrorDescription("Missing Agent Config File");
                return new ResponseEntity<String>(this.gson.toJson(verror), verror.getErrorCode());
            }
           
        }

        user = new User(userName, fullName, role, managerName, organization, qid, location);
        try {
            final User retrievedUser = this.userService.getUserByName(userName);
            if (retrievedUser == null) {
                this.userService.addUser(user);
            }
            else {
                this.userService.updateUser(retrievedUser, user);
            }
        }
        catch (ValidationError | IOException | InterruptedException | ExecutionException e) {
            logger.error("Failed to add/update the user to metastore " + e.getMessage());
        }
        return new ResponseEntity<User>(user, HttpStatus.OK);

    }

    @ApiOperation(value = "View a list of available Users", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("users")
    public ResponseEntity<?> getAllUsers() {    
		final List<User> list = this.userService.getAllUsers();
		return new ResponseEntity<List<User>>(list, HttpStatus.OK);
    }

    @ApiOperation(value = "Insert an User", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted User"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("user")
    public ResponseEntity<?> addUser(@RequestBody final User user) {
        User insertedUser;
        try {
            try {
                insertedUser = this.userService.addUser(user);
                return new ResponseEntity<String>(this.gson.toJson(insertedUser), HttpStatus.CREATED);
            }
            catch (IOException | InterruptedException | ExecutionException e) {
                final ValidationError verror = new ValidationError();
                verror.setErrorCode(HttpStatus.INTERNAL_SERVER_ERROR);
                verror.setErrorDescription(e.getMessage());
                return new ResponseEntity<ValidationError>(verror, verror.getErrorCode());
            }

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
    public ResponseEntity<?> deleteUser(@PathVariable("id") final Integer id) {
        try {
            User user;
            try {
                user = this.userService.deleteUser(id);
                return new ResponseEntity<String>(this.gson.toJson("Deleted " + user.getUserId()), HttpStatus.OK);
            }
            catch (IOException | InterruptedException | ExecutionException e) {
                final ValidationError verror = new ValidationError();
                verror.setErrorCode(HttpStatus.INTERNAL_SERVER_ERROR);
                verror.setErrorDescription(e.getMessage());
                return new ResponseEntity<ValidationError>(verror, verror.getErrorCode());
            }

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
    public ResponseEntity<?> activateUser(@PathVariable("id") final Integer id) {
        try {
            User user;
            try {
                user = this.userService.enableUser(id);
                return new ResponseEntity<String>(this.gson.toJson("Enabled " + user.getUserId()), HttpStatus.OK);
            }
            catch (IOException | InterruptedException | ExecutionException e) {
                final ValidationError verror = new ValidationError();
                verror.setErrorCode(HttpStatus.INTERNAL_SERVER_ERROR);
                verror.setErrorDescription(e.getMessage());
                return new ResponseEntity<ValidationError>(verror, verror.getErrorCode());
            }
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }
}
