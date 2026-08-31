package com.bancoxyz.batch.controller;

import com.bancoxyz.batch.service.BatchJobService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**

 * Controller REST encargado de iniciar manualmente los procesos batch
 * del Banco XYZ.
 *
 * Permite ejecutar el procesamiento completo o cada uno de los tres
 * procesos principales de manera independiente.
 */
@RestController
@RequestMapping("/api/batch")
@RequiredArgsConstructor
public class BatchJobController {

    private final BatchJobService batchJobService;

    /**

     * Ejecuta el procesamiento batch completo.
     *
     * @return información de la ejecución del Job.
     */
    @PostMapping("/procesar")
    public ResponseEntity<Map<String, Object>> procesarCompleto() {

        JobExecution execution =
                batchJobService.ejecutarProcesoCompleto();

        return ResponseEntity.ok(
                crearRespuesta(execution)
        );
    }

    /**

     * Ejecuta únicamente el proceso de transacciones diarias.
     *
     * @return información de la ejecución del Job.
     */
    @PostMapping("/transacciones")
    public ResponseEntity<Map<String, Object>> procesarTransacciones() {

        JobExecution execution =
                batchJobService.ejecutarTransacciones();

        return ResponseEntity.ok(
                crearRespuesta(execution)
        );
    }

    /**

     * Ejecuta únicamente el cálculo de intereses mensuales.
     *
     * @return información de la ejecución del Job.
     */
    @PostMapping("/intereses")
    public ResponseEntity<Map<String, Object>> procesarIntereses() {

        JobExecution execution =
                batchJobService.ejecutarIntereses();

        return ResponseEntity.ok(
                crearRespuesta(execution)
        );
    }

    /**

     * Ejecuta únicamente la generación de estados de cuenta anuales.
     *
     * @return información de la ejecución del Job.
     */
    @PostMapping("/estados-anuales")
    public ResponseEntity<Map<String, Object>> procesarEstadosAnuales() {

        JobExecution execution =
                batchJobService.ejecutarEstadosAnuales();

        return ResponseEntity.ok(
                crearRespuesta(execution)
        );
    }

    /**

     * Construye una respuesta común para todas las ejecuciones.
     */
    private Map<String, Object> crearRespuesta(
            JobExecution execution) {

        return Map.of(
                "jobExecutionId",
                execution.getId(),

                "jobInstanceId",
                execution.getJobInstance() != null
                        ? execution.getJobInstance().getInstanceId()
                        : null,

                "job",
                execution.getJobInstance() != null
                        ? execution.getJobInstance().getJobName()
                        : "desconocido",

                "estado",
                execution.getStatus().toString(),

                "exitStatus",
                execution.getExitStatus().getExitCode(),

                "inicio",
                execution.getStartTime() != null
                        ? execution.getStartTime().toString()
                        : null,

                "fin",
                execution.getEndTime() != null
                        ? execution.getEndTime().toString()
                        : null

        );
    }
}