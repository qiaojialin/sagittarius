package com.sagittarius.exceptions;

/**
 * Created by Leah on 2017/7/18.
 */
public class UnauthorizedException extends Exception {

    public UnauthorizedException(String message) {
        super(message);
    }

    public UnauthorizedException(String message, Throwable cause) {
        super(message, cause);
    }

}
