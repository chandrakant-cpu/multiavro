package com.multiavrostreamconsumer.model;

public class ErrorRecord {
    private final String record;
    private final String errorMessage;

    public ErrorRecord(String record, String errorMessage) {
        this.record = record;
        this.errorMessage = errorMessage;
    }

    public String getRecord() {
        return record;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return "ErrorRecord{" +
                "record='" + record + '\'' +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}