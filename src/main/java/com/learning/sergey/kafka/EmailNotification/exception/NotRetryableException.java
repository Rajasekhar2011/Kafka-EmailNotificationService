package com.learning.sergey.kafka.EmailNotification.exception;

public class NotRetryableException extends  RuntimeException{

    public NotRetryableException(String message) {
        super(message);
    }

    public NotRetryableException( Throwable cause) {
        super( cause);
    }

}
