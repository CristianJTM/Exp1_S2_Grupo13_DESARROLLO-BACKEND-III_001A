package com.bancoxyz.batch.processor;

import com.bancoxyz.batch.config.BatchDataConfig.InteresInput;
import com.bancoxyz.batch.config.BatchDataConfig.InteresProcesado;


import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class InteresProcessor
        implements ItemProcessor<InteresInput, InteresProcesado> {

    private static final BigDecimal TASA_AHORRO =
            new BigDecimal("0.02");

    private static final BigDecimal TASA_PRESTAMO =
            new BigDecimal("0.05");

    private static final BigDecimal TASA_HIPOTECA =
            new BigDecimal("0.04");

    @Override
    public InteresProcesado process(InteresInput item) {

        // ========================================================
        // INFORMACIÓN DEL HILO DE EJECUCIÓN
        // ========================================================

        System.out.println(
                "CUENTA " + item.cuentaId()
                        + " -> Hilo: "
                        + Thread.currentThread().getName()
        );

        // ========================================================
        // VALIDACIÓN DEL SALDO
        // ========================================================

        if (item.saldo() == null) {
            return null;
        }

        if (item.saldo().compareTo(BigDecimal.ZERO) < 0) {
            return null;
        }

        // ========================================================
        // VALIDACIÓN DE EDAD
        // ========================================================

        if (item.edad() == null ||
                item.edad() < 18 ||
                item.edad() > 100) {

            return null;
        }

        // ========================================================
        // VALIDACIÓN DEL TIPO DE CUENTA
        // ========================================================

        if (item.tipo() == null) {
            return null;
        }

        String tipo = item.tipo()
                .trim()
                .toLowerCase();

        BigDecimal tasa;

        switch (tipo) {

            case "ahorro" -> tasa = TASA_AHORRO;

            case "prestamo" -> tasa = TASA_PRESTAMO;

            case "hipoteca" -> tasa = TASA_HIPOTECA;

            default -> {
                return null;
            }
        }

        // ========================================================
        // CÁLCULO DEL INTERÉS
        // ========================================================

        BigDecimal interes = item.saldo()
                .multiply(tasa)
                .setScale(2);

        // ========================================================
        // CÁLCULO DEL SALDO FINAL
        // ========================================================

        BigDecimal saldoFinal;

        if ("ahorro".equals(tipo)) {

            // Para ahorro, el interés incrementa el saldo.
            saldoFinal = item.saldo()
                    .add(interes);

        } else {

            /*
             * Para préstamos e hipotecas se mantiene el interés
             * como parte del saldo a pagar.
             */
            saldoFinal = item.saldo()
                    .add(interes);
        }

        // ========================================================
        // RESULTADO
        // ========================================================

        return new InteresProcesado(
                item.cuentaId(),
                item.nombre(),
                item.saldo(),
                item.edad(),
                tipo,
                tasa,
                interes,
                saldoFinal
        );
    }
}
