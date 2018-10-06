package com.paypal.udc.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import com.paypal.udc.interceptor.UDCInterceptor;


@Configuration
public class UDCInterceptorConfig extends WebMvcConfigurerAdapter {

    @Autowired
    UDCInterceptor udcInterceptor;

    @Override
    public void addInterceptors(final InterceptorRegistry registry) {
        registry.addInterceptor(this.udcInterceptor);
    }
}
