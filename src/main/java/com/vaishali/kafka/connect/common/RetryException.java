package com.vaishali.kafka.connect.common;

/**
 * Created by vikas.tikoo on 7/12/17.
 */
public class RetryException extends RuntimeException{
    public RetryException(int numberOfFailedAttempts) {
        super("Retrying failed  after " + numberOfFailedAttempts + " attempts.");
    }
}
