package com.bancoxyz.batch.tasklet;

import com.bancoxyz.batch.model.AnomaliaTransaccion;
import com.bancoxyz.batch.model.ResumenTransacciones;
import com.bancoxyz.batch.model.Transaccion;
import com.bancoxyz.batch.repository.AnomaliaTransaccionRepository;
import com.bancoxyz.batch.repository.ResumenTransaccionesRepository;
import com.bancoxyz.batch.repository.TransaccionRepository;

import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class ResumenAnomaliasTasklet implements Tasklet {

    private final TransaccionRepository transaccionRepository;
    private final AnomaliaTransaccionRepository anomaliaTransaccionRepository;
    private final ResumenTransaccionesRepository resumenTransaccionesRepository;

    public ResumenAnomaliasTasklet(
            TransaccionRepository transaccionRepository,
            AnomaliaTransaccionRepository anomaliaTransaccionRepository,
            ResumenTransaccionesRepository resumenTransaccionesRepository) {

        this.transaccionRepository = transaccionRepository;
        this.anomaliaTransaccionRepository =
                anomaliaTransaccionRepository;
        this.resumenTransaccionesRepository =
                resumenTransaccionesRepository;
    }

    @Override
    public RepeatStatus execute(
            StepContribution contribution,
            ChunkContext chunkContext) {

        // ========================================================
        // OBTENER TRANSACCIONES VÁLIDAS
        // ========================================================

        List<Transaccion> transaccionesValidas =
                transaccionRepository.findAll();

        // ========================================================
        // OBTENER TRANSACCIONES CON ANOMALÍAS
        // ========================================================

        List<AnomaliaTransaccion> transaccionesAnomalas =
                anomaliaTransaccionRepository.findAll();

        // ========================================================
        // CONTADORES GENERALES
        // ========================================================

        int totalValidas =
                transaccionesValidas.size();

        int totalAnomalias =
                transaccionesAnomalas.size();

        int totalTransacciones =
                totalValidas + totalAnomalias;

        // ========================================================
        // PORCENTAJE DE ANOMALÍAS
        // ========================================================

        double porcentajeAnomalias = 0;

        if (totalTransacciones > 0) {

            porcentajeAnomalias =
                    (totalAnomalias * 100.0)
                            / totalTransacciones;
        }

        // ========================================================
        // CONSOLIDAR TIPOS DE ANOMALÍAS
        // ========================================================

        Map<String, Long> resumenAnomalias =
                transaccionesAnomalas.stream()
                        .collect(
                                Collectors.groupingBy(
                                        AnomaliaTransaccion::getDescripcion,
                                        Collectors.counting()
                                )
                        );

        // ========================================================
        // GENERAR OBSERVACIÓN DEL RESUMEN
        // ========================================================

        String observacion;

        if (resumenAnomalias.isEmpty()) {

            observacion =
                    "Procesamiento completado sin anomalías.";

        } else {

            observacion =
                    resumenAnomalias.entrySet()
                            .stream()
                            .map(entry ->
                                    entry.getKey()
                                            + " -> "
                                            + entry.getValue()
                            )
                            .collect(
                                    Collectors.joining("; ")
                            );
        }

        // ========================================================
        // GUARDAR RESUMEN EN LA BASE DE DATOS
        // ========================================================

        ResumenTransacciones resumen =
                new ResumenTransacciones(
                        LocalDate.now(),
                        observacion,
                        totalTransacciones,
                        totalAnomalias,
                        totalValidas
                );

        resumenTransaccionesRepository.save(resumen);

        // ========================================================
        // MOSTRAR RESUMEN EN CONSOLA
        // ========================================================

        System.out.println();
        System.out.println(
                "========================================"
        );
        System.out.println(
                "       RESUMEN DE ANOMALÍAS"
        );
        System.out.println(
                "========================================"
        );

        System.out.println(
                "Total de transacciones procesadas: "
                        + totalTransacciones
        );

        System.out.println(
                "Transacciones válidas: "
                        + totalValidas
        );

        System.out.println(
                "Transacciones con anomalías: "
                        + totalAnomalias
        );

        System.out.printf(
                "Porcentaje de anomalías: %.2f%%%n",
                porcentajeAnomalias
        );

        System.out.println();
        System.out.println(
                "Detalle de anomalías:"
        );

        if (resumenAnomalias.isEmpty()) {

            System.out.println(
                    "- No se detectaron anomalías."
            );

        } else {

            resumenAnomalias.forEach(
                    (descripcion, cantidad) -> {

                        System.out.println(
                                "- "
                                        + descripcion
                                        + " -> "
                                        + cantidad
                        );
                    }
            );
        }

        System.out.println();
        System.out.println(
                "Resumen guardado en "
                        + "resumen_transacciones."
        );

        System.out.println(
                "========================================"
        );
        System.out.println();

        return RepeatStatus.FINISHED;
    }
}