package com.paypal.udc.interceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;


@Component
public class UDCInterceptor implements HandlerInterceptor {

    final static Logger logger = LoggerFactory.getLogger(UDCInterceptor.class);
    @Value("${application.env}")
    private String isProd;

    public String getIsProd() {
        return this.isProd;
    }

    public void setIsProd(final String isProd) {
        this.isProd = isProd;
    }

    @Override
    public void afterCompletion(final HttpServletRequest request, final HttpServletResponse response,
            final Object object, final Exception exception)
            throws Exception {

    }

    @Override
    public void postHandle(final HttpServletRequest request, final HttpServletResponse response, final Object object,
            final ModelAndView mvc)
            throws Exception {
    }

    @Override
    public boolean preHandle(final HttpServletRequest request, final HttpServletResponse response, final Object object)
            throws Exception {

        return true;
    }
}
