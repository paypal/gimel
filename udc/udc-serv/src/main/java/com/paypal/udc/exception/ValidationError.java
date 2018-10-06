package com.paypal.udc.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;


public class ValidationError extends Exception {

    private static final long serialVersionUID = -5166164364710079372L;
    final static Logger logger = LoggerFactory.getLogger(ValidationError.class);
    HttpStatus errorCode;
    String errorDescription;

    public ValidationError() {
        this.errorCode = HttpStatus.OK;
        this.errorDescription = "";
    }

    public HttpStatus getErrorCode() {
        return this.errorCode;
    }

    public void setErrorCode(final HttpStatus errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorDescription() {
        return this.errorDescription;
    }

    public void setErrorDescription(final String errorDescription) {
        this.errorDescription = errorDescription;
    }

    public static Error getError(final String error_log) {

        logger.error(error_log);
        final Error error = new Error(error_log);
        return error;
    }

}
