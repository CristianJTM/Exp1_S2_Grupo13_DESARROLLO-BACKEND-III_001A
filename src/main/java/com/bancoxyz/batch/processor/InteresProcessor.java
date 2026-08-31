package com.bancoxyz.batch.processor;

import com.bancoxyz.batch.config.BatchDataConfig.InteresInput;
import com.bancoxyz.batch.config.BatchDataConfig.InteresProcesado;
import com.bancoxyz.batch.exception.DatoInvalidoException;

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
    public InteresProcesado process(InteresInput item)
            throws DatoInvalidoException {

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

            throw new DatoInvalidoException(
                    "Cuenta " + item.cuentaId()
                            + ": saldo inexistente."
            );
        }

        if (item.saldo().compareTo(BigDecimal.ZERO) < 0) {

            throw new DatoInvalidoException(
                    "Cuenta " + item.cuentaId()
                            + ": saldo inválido, no puede ser negativo."
            );
        }

        // ========================================================
        // VALIDACIÓN DE EDAD
        // ========================================================

        if (item.edad() == null) {

            throw new DatoInvalidoException(
                    "Cuenta " + item.cuentaId()
                            + ": edad inexistente."
            );
        }

        if (item.edad() < 18 || item.edad() > 100) {

            throw new DatoInvalidoException(
                    "Cuenta " + item.cuentaId()
                            + ": edad inválida, debe estar entre 18 y 100 años."
            );
        }

        // ========================================================
        // VALIDACIÓN DEL TIPO DE CUENTA
        // ========================================================

        if (item.tipo() == null ||
                item.tipo().isBlank()) {

            throw new DatoInvalidoException(
                    "Cuenta " + item.cuentaId()
                            + ": tipo de cuenta inexistente."
            );
        }

        String tipo = item.tipo()
                .trim()
                .toLowerCase();

        BigDecimal tasa;

        switch (tipo) {

            case "ahorro" -> tasa = TASA_AHORRO;

            case "prestamo" -> tasa = TASA_PRESTAMO;

            case "hipoteca" -> tasa = TASA_HIPOTECA;

            default -> throw new DatoInvalidoException(
                    "Cuenta " + item.cuentaId()
                            + ": tipo de cuenta inválido: "
                            + item.tipo()
            );
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

        BigDecimal saldoFinal =
                item.saldo().add(interes);

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