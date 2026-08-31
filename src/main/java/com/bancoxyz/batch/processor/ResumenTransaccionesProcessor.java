package com.bancoxyz.batch.processor;

import com.bancoxyz.batch.model.Transaccion;
import com.bancoxyz.batch.model.ResumenTransacciones;
import com.bancoxyz.batch.repository.TransaccionRepository;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;

@Component
public class ResumenTransaccionesProcessor
        implements ItemProcessor<LocalDate, ResumenTransacciones> {

    private final TransaccionRepository transaccionRepository;

    public ResumenTransaccionesProcessor(
            TransaccionRepository transaccionRepository) {

        this.transaccionRepository =
                transaccionRepository;
    }

    @Override
    public ResumenTransacciones process(
            LocalDate fecha) {

        // ========================================================
        // OBTENER TRANSACCIONES DE LA FECHA
        // ========================================================

        List<Transaccion> transacciones =
                transaccionRepository.findByFecha(fecha);

        // ========================================================
        // CONTADORES
        // ========================================================

        int total =
                transacciones.size();

        int anomalas =
                (int) transacciones.stream()
                        .filter(Transaccion::isAnomalia)
                        .count();

        int validas =
                total - anomalas;

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