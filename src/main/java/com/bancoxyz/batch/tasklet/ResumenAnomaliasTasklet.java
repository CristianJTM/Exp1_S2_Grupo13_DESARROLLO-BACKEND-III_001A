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
import java.util.LinkedHashMap;

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
        // CLASIFICAR ANOMALÍAS POR TIPO
        // ========================================================

        Map<String, Long> resumenPorTipo =
                new LinkedHashMap<>();

        for (AnomaliaTransaccion anomalia :
                transaccionesAnomalas) {

            String descripcion =
                    anomalia.getDescripcion();

            String tipo;

            if (descripcion.contains(
                    "monto inexistente")) {

                tipo = "Monto inexistente";

            } else if (descripcion.contains(
                    "monto inválido")) {

                tipo = "Monto inválido";

            } else if (descripcion.contains(
                    "fecha inexistente")) {

                tipo = "Fecha inexistente";

            } else if (descripcion.contains(
                    "tipo de transacción inválido")) {

                tipo = "Tipo de transacción inválido";

            } else {

                tipo = "Otros errores";
            }

            resumenPorTipo.merge(
                    tipo,
                    1L,
                    Long::sum
            );
        }

        // ========================================================
        // GENERAR OBSERVACIÓN PARA LA BASE DE DATOS
        // ========================================================

        String observacion;

        if (resumenPorTipo.isEmpty()) {

            observacion =
                    "Procesamiento completado sin anomalías.";

        } else {

            observacion =
                    resumenPorTipo.entrySet()
                            .stream()
                            .map(entry ->
                                    entry.getKey()
                                            + ": "
                                            + entry.getValue()
                            )
                            .reduce(
                                    (a, b) -> a + "; " + b
                            )
                            .orElse(
                                    "Procesamiento completado."
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

        // ========================================================
        // RESUMEN POR TIPO
        // ========================================================

        System.out.println();
        System.out.println(
                "Anomalías por tipo:"
        );

        if (resumenPorTipo.isEmpty()) {

            System.out.println(
                    "- No se detectaron anomalías."
            );

        } else {

            resumenPorTipo.forEach(
                    (tipo, cantidad) -> {

                        System.out.println(
                                "- "
                                        + tipo
                                        + ": "
                                        + cantidad
                        );
                    }
            );
        }

        // ========================================================
        // DETALLE DE ANOMALÍAS
        // ========================================================

        System.out.println();
        System.out.println(
                "Detalle de anomalías:"
        );

        if (transaccionesAnomalas.isEmpty()) {

            System.out.println(
                    "- No se detectaron anomalías."
            );

        } else {

            transaccionesAnomalas.forEach(
                    anomalia -> {

                        System.out.println(
                                "- "
                                        + anomalia.getDescripcion()
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