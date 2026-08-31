package com.bancoxyz.batch.service;

import com.bancoxyz.batch.exception.BatchJobLaunchException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**

 * Servicio encargado de iniciar los Jobs de Spring Batch.
 *
 * Centraliza la ejecución de los procesos batch y evita que el Controller
 * tenga que conocer directamente la API interna de Spring Batch.
 *
 * Utiliza JobOperator, que es la alternativa recomendada a JobLauncher
 * en Spring Batch 6.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BatchJobService {

    private final JobOperator jobOperator;

    @Qualifier("procesoBatchCompleto")
    private final Job procesoBatchCompleto;

    @Qualifier("transaccionesJob")
    private final Job transaccionesJob;

    @Qualifier("interesesJob")
    private final Job interesesJob;

    @Qualifier("estadosAnualesJob")
    private final Job estadosAnualesJob;

    /**

     * Ejecuta el procesamiento completo del Banco XYZ.
     *
     * Incluye los tres procesos principales:
     * transacciones, intereses y estados anuales.
     *
     * @return información de la ejecución del Job.
     */
    public JobExecution ejecutarProcesoCompleto() {

        return ejecutarJob(
                procesoBatchCompleto,
                "procesoBatchCompleto"
        );
    }

    /**

     * Ejecuta únicamente el proceso de transacciones diarias.
     *
     * @return información de la ejecución del Job.
     */
    public JobExecution ejecutarTransacciones() {

        return ejecutarJob(
                transaccionesJob,
                "transaccionesJob"
        );
    }

    /**

     * Ejecuta únicamente el cálculo de intereses mensuales.
     *
     * @return información de la ejecución del Job.
     */
    public JobExecution ejecutarIntereses() {

        return ejecutarJob(
                interesesJob,
                "interesesJob"
        );
    }

    /**

     * Ejecuta únicamente la generación de estados de cuenta anuales.
     *
     * @return información de la ejecución del Job.
     */
    public JobExecution ejecutarEstadosAnuales() {

        return ejecutarJob(
                estadosAnualesJob,
                "estadosAnualesJob"
        );
    }

    /**

     * Método común para ejecutar cualquier Job.
     *
     * Genera parámetros únicos para cada ejecución para permitir
     * ejecutar nuevamente el mismo Job.
     */
    private JobExecution ejecutarJob(
            Job job,
            String nombreJob) {

        try {

            JobParametersBuilder parametersBuilder =
                    new JobParametersBuilder();

            parametersBuilder.addLong(
                    "timestamp",
                    System.currentTimeMillis()
            );

            JobExecution execution =
                    jobOperator.start(
                            job,
                            parametersBuilder.toJobParameters()
                    );

            log.info(
                    "Job {} ejecutado. Execution ID: {}. Estado: {}",
                    nombreJob,
                    execution.getId(),
                    execution.getStatus()
            );

            return execution;

        } catch (Exception ex) {

            log.error(
                    "No fue posible iniciar el Job {}",
                    nombreJob,
                    ex
            );

            throw new BatchJobLaunchException(
                    "No fue posible iniciar el Job "
                            + nombreJob
                            + ": "
                            + ex.getMessage(),
                    ex
            );
        }
    }
}
