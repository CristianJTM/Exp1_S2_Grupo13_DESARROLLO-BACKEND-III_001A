package com.bancoxyz.batch.processor;

import com.bancoxyz.batch.config.BatchDataConfig.TransaccionInput;
import com.bancoxyz.batch.config.BatchDataConfig.TransaccionProcesada;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class TransaccionProcessor
        implements ItemProcessor<TransaccionInput, TransaccionProcesada> {

    @Override
    public TransaccionProcesada process(TransaccionInput item) {

        boolean anomalia = false;
        StringBuilder observacion = new StringBuilder();

        // ========================================================
        // INFORMACIÓN DEL HILO DE EJECUCIÓN
        // ========================================================

        System.out.println(
                "TRANSACCIÓN " + item.id()
                        + " -> Hilo: "
                        + Thread.currentThread().getName()
        );

        // ========================================================
        // VALIDACIÓN DEL MONTO
        // ========================================================

        if (item.monto() == null) {

            anomalia = true;
            observacion.append("Monto inexistente. ");

        } else if (item.monto().compareTo(BigDecimal.ZERO) <= 0) {

            anomalia = true;
            observacion.append(
                    "Monto inválido: debe ser mayor que cero. "
            );
        }

        // ========================================================
        // VALIDACIÓN DEL TIPO
        // ========================================================

        if (item.tipo() == null ||
                (!"debito".equalsIgnoreCase(item.tipo()) &&
                        !"credito".equalsIgnoreCase(item.tipo()))) {

            anomalia = true;
            observacion.append(
                    "Tipo de transacción inválido. "
            );
        }

        // ========================================================
        // VALIDACIÓN DE FECHA
        // ========================================================

        if (item.fecha() == null) {

            anomalia = true;
            observacion.append(
                    "Fecha inválida o inexistente. "
            );
        }

        // ========================================================
        // RESULTADO
        // ========================================================

        String resultado = observacion.length() == 0
                ? "Transacción válida"
                : observacion.toString().trim();

        return new TransaccionProcesada(
                item.id(),
                item.fecha(),
                item.monto(),
                item.tipo(),
                anomalia,
                resultado
        );
    }
}
