package com.sagittarius.exceptions;

/**
 * Created by Leah on 2017/7/11.
 */
public class DataTypeMismatchException extends Exception {

    public DataTypeMismatchException(String message) {
        super(message);
    }

    public DataTypeMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

}
