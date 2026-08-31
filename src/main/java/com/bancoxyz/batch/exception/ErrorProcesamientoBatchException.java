package com.bancoxyz.batch.exception;

public class ErrorProcesamientoBatchException extends RuntimeException {

    public ErrorProcesamientoBatchException(String mensaje) {
        super(mensaje);
    }

    public ErrorProcesamientoBatchException(String mensaje, Throwable causa) {
        super(mensaje, causa);
    }
}
