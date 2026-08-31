package com.bancoxyz.batch.processor;

import com.bancoxyz.batch.config.BatchDataConfig.CuentaAnualInput;
import com.bancoxyz.batch.config.BatchDataConfig.CuentaAnualProcesada;
import com.bancoxyz.batch.exception.DatoInvalidoException;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class CuentaAnualProcessor
        implements ItemProcessor<CuentaAnualInput, CuentaAnualProcesada> {

    @Override
    public CuentaAnualProcesada process(
            CuentaAnualInput item)
            throws DatoInvalidoException {

        // ========================================================
        // INFORMACIÓN DEL HILO DE EJECUCIÓN
        // ========================================================

        System.out.println(
                "CUENTA ANUAL " + item.cuentaId()
                        + " -> Hilo: "
                        + Thread.currentThread().getName()
        );

        // ========================================================
        // VALIDACIÓN DE CUENTA
        // ========================================================

        if (item.cuentaId() == null) {

            throw new DatoInvalidoException(
                    "Cuenta anual: identificador de cuenta inexistente."
            );
        }

        // ========================================================
        // VALIDACIÓN DE FECHA
        // ========================================================

        if (item.fecha() == null) {

            throw new DatoInvalidoException(
                    "Cuenta " + item.cuentaId()
                            + ": fecha inexistente."
            );
        }

        // ========================================================
        // VALIDACIÓN DEL MONTO
        // ========================================================

        if (item.monto() == null) {

            throw new DatoInvalidoException(
                    "Cuenta " + item.cuentaId()
                            + ": monto inexistente."
            );
        }

        if (item.monto().compareTo(BigDecimal.ZERO) == 0) {

            throw new DatoInvalidoException(
                    "Cuenta " + item.cuentaId()
                            + ": monto inválido, no puede ser cero."
            );
        }

        // ========================================================
        // VALIDACIÓN DE DESCRIPCIÓN
        // ========================================================

        if (item.descripcion() == null ||
                item.descripcion().isBlank()) {

            throw new DatoInvalidoException(
                    "Cuenta " + item.cuentaId()
                            + ": descripción inexistente."
            );
        }

        // ========================================================
        // VALIDACIÓN DEL TIPO DE TRANSACCIÓN
        // ========================================================

        if (item.transaccion() == null ||
                item.transaccion().isBlank()) {

            throw new DatoInvalidoException(
                    "Cuenta " + item.cuentaId()
                            + ": tipo de transacción inexistente."
            );
        }

        String tipo = item.transaccion()
                .trim()
                .toLowerCase();

        if (!"deposito".equals(tipo) &&
                !"retiro".equals(tipo) &&
                !"compra".equals(tipo)) {

            throw new DatoInvalidoException(
                    "Cuenta " + item.cuentaId()
                            + ": tipo de transacción inválido: "
                            + item.transaccion()
            );
        }

        // ========================================================
        // INICIALIZACIÓN DE TOTALES
        // ========================================================

        BigDecimal totalDepositos = BigDecimal.ZERO;
        BigDecimal totalRetiros = BigDecimal.ZERO;

        // ========================================================
        // CLASIFICACIÓN DEL MOVIMIENTO
        // ========================================================

        if ("deposito".equals(tipo)) {

            if (item.monto().compareTo(BigDecimal.ZERO) <= 0) {

                throw new DatoInvalidoException(
                        "Cuenta " + item.cuentaId()
                                + ": un depósito debe tener monto positivo."
                );
            }

            totalDepositos = item.monto();

        } else if ("retiro".equals(tipo) ||
                "compra".equals(tipo)) {

            if (item.monto().compareTo(BigDecimal.ZERO) >= 0) {

                throw new DatoInvalidoException(
                        "Cuenta " + item.cuentaId()
                                + ": un retiro o compra debe tener monto negativo."
                );
            }

            totalRetiros = item.monto().abs();
        }

        // ========================================================
        // SALDO DEL MOVIMIENTO
        // ========================================================

        BigDecimal saldoMovimiento = item.monto();

        // ========================================================
        // RESULTADO
        // ========================================================

        return new CuentaAnualProcesada(
                item.cuentaId(),
                item.fecha().getYear(),
                totalDepositos,
                totalRetiros,
                saldoMovimiento,
                1,
                "Operación procesada"
        );
    }
}