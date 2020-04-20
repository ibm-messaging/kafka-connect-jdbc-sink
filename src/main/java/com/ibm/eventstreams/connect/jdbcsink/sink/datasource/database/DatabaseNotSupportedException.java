package com.ibm.eventstreams.connect.jdbcsink.sink.datasource.database;

public class DatabaseNotSupportedException extends RuntimeException {
    public DatabaseNotSupportedException(String message){
        super(message);
    }
}
