package com.bancoxyz.batch.processor;

import com.bancoxyz.batch.config.BatchDataConfig.TransaccionInput;
import com.bancoxyz.batch.config.BatchDataConfig.TransaccionProcesada;
import com.bancoxyz.batch.exception.DatoInvalidoException;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class TransaccionProcessor
        implements ItemProcessor<TransaccionInput, TransaccionProcesada> {

    @Override
    public TransaccionProcesada process(TransaccionInput item)
            throws DatoInvalidoException {

        // ========================================================
        // INFORMACIÓN DEL HILO DE EJECUCIÓN
        // ========================================================

        System.out.println(
                "TRANSACCIÓN " + item.id()
                        + " -> Hilo: "
                        + Thread.currentThread().getName()
        );

        // ========================================================
        // VALIDACIÓN DE FECHA
        // ========================================================

        if (item.fecha() == null) {

            throw new DatoInvalidoException(
                    "Transacción " + item.id()
                            + ": fecha inexistente."
            );
        }

        // ========================================================
        // VALIDACIÓN DEL MONTO
        // ========================================================

        if (item.monto() == null) {

            throw new DatoInvalidoException(
                    "Transacción " + item.id()
                            + ": monto inexistente."
            );
        }

        if (item.monto().compareTo(BigDecimal.ZERO) <= 0) {

            throw new DatoInvalidoException(
                    "Transacción " + item.id()
                            + ": monto inválido, debe ser mayor que cero."
            );
        }

        // ========================================================
        // VALIDACIÓN DEL TIPO
        // ========================================================

        if (item.tipo() == null ||
                (!"debito".equalsIgnoreCase(item.tipo()) &&
                        !"credito".equalsIgnoreCase(item.tipo()))) {

            throw new DatoInvalidoException(
                    "Transacción " + item.id()
                            + ": tipo de transacción inválido."
            );
        }

        // ========================================================
        // RESULTADO
        // ========================================================

        return new TransaccionProcesada(
                item.id(),
                item.fecha(),
                item.monto(),
                item.tipo(),
                false,
                "Transacción válida"
        );
    }
}