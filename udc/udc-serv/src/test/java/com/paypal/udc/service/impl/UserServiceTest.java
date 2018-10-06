package com.paypal.udc.service.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import com.paypal.udc.config.UDCInterceptorConfig;
import com.paypal.udc.dao.UserRepository;
import com.paypal.udc.entity.User;
import com.paypal.udc.interceptor.UDCInterceptor;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.user.UserFullNameValidator;
import com.paypal.udc.validator.user.UserNameValidator;


@RunWith(SpringRunner.class)
public class UserServiceTest {

    @MockBean
    private UDCInterceptor udcInterceptor;

    @MockBean
    private UDCInterceptorConfig udcInterceptorConfig;

    @MockBean
    private UserUtil userUtil;

    @Mock
    private UserRepository userRepository;

    @Mock
    private UserFullNameValidator s2;

    @Mock
    private UserNameValidator s1;

    @InjectMocks
    private UserService userService;

    private long userId;
    private String userName;
    private User user;
    private List<User> userList;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.userId = 0L;
        this.userName = "userName";
        this.user = new User(this.userName, "userFullName", "roles", "managerName", "organization", "qid");
        this.userList = Arrays.asList(this.user);
    }

    @Test
    public void verifyValidGetAllUsers() throws Exception {
        when(this.userRepository.findAll()).thenReturn(this.userList);

        final List<User> result = this.userService.getAllUsers();
        assertEquals(this.userList.size(), result.size());

        verify(this.userRepository).findAll();
    }

    @Test
    public void verifyValidGetUserById() throws Exception {
        when(this.userRepository.findOne(this.userId)).thenReturn(this.user);

        final User result = this.userService.getUserById(this.userId);
        assertEquals(this.user, result);

        verify(this.userRepository).findOne(this.userId);
    }

    @Test
    public void verifyValidGetUserByName() throws Exception {
        when(this.userRepository.findByUserName(this.userName)).thenReturn(this.user);

        final User result = this.userService.getUserByName(this.userName);
        assertEquals(this.user, result);

        verify(this.userRepository).findByUserName(this.userName);
    }

    @Test
    public void verifyValidDeleteUser() throws Exception {
        when(this.userRepository.findOne(this.userId)).thenReturn(this.user);
        when(this.userRepository.save(this.user)).thenReturn(this.user);

        final User result = this.userService.deleteUser(this.userId);
        assertEquals(this.user, result);
        assertEquals(ActiveEnumeration.NO.getFlag(), result.getIsActiveYN());

        verify(this.userRepository).save(this.user);
    }

    @Test
    public void verifyValidEnableUser() throws Exception {
        when(this.userRepository.findOne(this.userId)).thenReturn(this.user);
        when(this.userRepository.save(this.user)).thenReturn(this.user);

        final User result = this.userService.enableUser(this.userId);
        assertEquals(this.user, result);
        assertEquals(ActiveEnumeration.YES.getFlag(), result.getIsActiveYN());

        verify(this.userRepository).save(this.user);
    }

    @Test
    public void verifyValidAddUser() throws Exception {
        when(this.userRepository.save(this.user)).thenReturn(this.user);

        final User result = this.userService.addUser(this.user);
        assertEquals(this.user, result);
        assertEquals(ActiveEnumeration.YES.getFlag(), result.getIsActiveYN());

        verify(this.userRepository).save(this.user);
    }

    @Test
    public void verifyValidUpdateUser() throws Exception {
        when(this.userRepository.findOne(this.userId)).thenReturn(this.user);
        when(this.userRepository.save(this.user)).thenReturn(this.user);

        final User result = this.userService.updateUser(this.user);
        assertEquals(this.user, result);

        verify(this.userRepository).save(this.user);
    }
}
