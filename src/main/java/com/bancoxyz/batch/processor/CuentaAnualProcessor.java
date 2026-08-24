package com.bancoxyz.batch.processor;

import com.bancoxyz.batch.config.BatchDataConfig.CuentaAnualInput;
import com.bancoxyz.batch.config.BatchDataConfig.CuentaAnualProcesada;


import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class CuentaAnualProcessor
        implements ItemProcessor<CuentaAnualInput, CuentaAnualProcesada> {

    @Override
    public CuentaAnualProcesada process(
            CuentaAnualInput item) {

        // ========================================================
        // VALIDACIÓN DE CUENTA
        // ========================================================

        if (item.cuentaId() == null) {
            return null;
        }

        // ========================================================
        // VALIDACIÓN DE FECHA
        // ========================================================

        if (item.fecha() == null) {
            return null;
        }

        // ========================================================
        // VALIDACIÓN DEL MONTO
        // ========================================================

        if (item.monto() == null) {
            return null;
        }

        if (item.monto().compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }

        // ========================================================
        // VALIDACIÓN DE DESCRIPCIÓN
        // ========================================================

        if (item.descripcion() == null ||
                item.descripcion().isBlank()) {

            return null;
        }

        // ========================================================
        // VALIDACIÓN DEL TIPO DE TRANSACCIÓN
        // ========================================================

        if (item.transaccion() == null) {
            return null;
        }

        String tipo = item.transaccion()
                .trim()
                .toLowerCase();

        if (!"deposito".equals(tipo) &&
                !"retiro".equals(tipo) &&
                !"compra".equals(tipo)) {

            return null;
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

            if (item.monto().compareTo(BigDecimal.ZERO) > 0) {

                totalDepositos = item.monto();
            }

        } else if ("retiro".equals(tipo) ||
                "compra".equals(tipo)) {

            if (item.monto().compareTo(BigDecimal.ZERO) < 0) {

                totalRetiros = item.monto().abs();
            }
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
