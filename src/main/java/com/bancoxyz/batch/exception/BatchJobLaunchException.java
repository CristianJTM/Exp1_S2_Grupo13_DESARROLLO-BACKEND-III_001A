package com.bancoxyz.batch.exception;

/**

 * Excepción utilizada cuando un Job de Spring Batch
 * no puede ser iniciado correctamente.
 */
public class BatchJobLaunchException extends RuntimeException {

    public BatchJobLaunchException(
            String message,
            Throwable cause) {

        super(message, cause);

    }
}
