package com.sagittarius.exceptions;

/**
 * Created by Leah on 2017/5/15.
 */
public class UnregisteredHostMetricException extends Exception {

    public UnregisteredHostMetricException(String message) {
        super(message);
    }

    public UnregisteredHostMetricException(String message, Throwable cause) {
        super(message, cause);
    }

}
