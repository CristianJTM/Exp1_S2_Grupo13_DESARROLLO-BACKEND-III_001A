package com.bancoxyz.batch.processor;

import com.bancoxyz.batch.model.ResumenTransacciones;
import com.bancoxyz.batch.repository.AnomaliaTransaccionRepository;
import com.bancoxyz.batch.repository.TransaccionRepository;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

@Component
public class ResumenTransaccionesProcessor
        implements ItemProcessor<LocalDate, ResumenTransacciones> {

    private final TransaccionRepository transaccionRepository;
    private final AnomaliaTransaccionRepository anomaliaTransaccionRepository;

    public ResumenTransaccionesProcessor(
            TransaccionRepository transaccionRepository,
            AnomaliaTransaccionRepository anomaliaTransaccionRepository) {

        this.transaccionRepository = transaccionRepository;
        this.anomaliaTransaccionRepository = anomaliaTransaccionRepository;
    }

    @Override
    public ResumenTransacciones process(LocalDate fecha) {

        // ========================================================
        // OBTENER CANTIDAD DE TRANSACCIONES VÁLIDAS
        // ========================================================

        int validas =
                transaccionRepository.findByFecha(fecha).size();

        // ========================================================
        // OBTENER CANTIDAD DE ANOMALÍAS
        // ========================================================

        int anomalas =
                anomaliaTransaccionRepository.findByFechaTransaccion(fecha).size();

        // ========================================================
        // TOTAL DE TRANSACCIONES PROCESADAS
        // ========================================================

        int total = validas + anomalas;

        // ========================================================
        // GENERAR OBSERVACIÓN
        // ========================================================

        String observacion;

        if (anomalas > 0) {

            observacion =
                    "Se detectaron "
                            + anomalas
                            + " transacciones con anomalías.";

        } else {

            observacion =
                    "No se detectaron anomalías.";
        }

        // ========================================================
        // RESULTADO
        // ========================================================

        return new ResumenTransacciones(
                fecha,
                observacion,
                total,
                anomalas,
                validas
        );
    }
}