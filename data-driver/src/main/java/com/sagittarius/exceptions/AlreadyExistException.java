package com.sagittarius.exceptions;

/**
 * Created by Leah on 2017/7/18.
 */
public class AlreadyExistException extends Exception{
    public AlreadyExistException(String message) {
        super(message);
    }

    public AlreadyExistException(String message, Throwable cause) {
        super(message, cause);
    }
}
