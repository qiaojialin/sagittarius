package com.sagittarius.exceptions;

/**
 * Created by Leah on 2017/7/18.
 */
public class AuthenticationException extends Exception{

    public AuthenticationException(String message) {
        super(message);
    }

    public AuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }
}
