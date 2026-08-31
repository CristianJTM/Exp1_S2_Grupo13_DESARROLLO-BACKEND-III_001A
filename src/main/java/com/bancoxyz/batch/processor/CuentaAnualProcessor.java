package com.bancoxyz.batch.processor;

import com.bancoxyz.batch.config.BatchDataConfig.CuentaAnualInput;
import com.bancoxyz.batch.config.BatchDataConfig.CuentaAnualProcesada;
import com.bancoxyz.batch.exception.DatoInvalidoException;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.Normalizer;

@Component
public class CuentaAnualProcessor
        implements ItemProcessor<CuentaAnualInput, CuentaAnualProcesada> {

    @Override
    public CuentaAnualProcesada process(
            CuentaAnualInput item)
            throws DatoInvalidoException {


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

        // ========================================================
        // NORMALIZACIÓN DEL TIPO
        // ========================================================

        String tipo = Normalizer
                .normalize(
                        item.transaccion(),
                        Normalizer.Form.NFD
                )
                .replaceAll("\\p{M}", "")
                .toLowerCase()
                .trim();

        // ========================================================
        // VALIDACIÓN DEL TIPO
        // ========================================================

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
        BigDecimal saldoMovimiento;

        // ========================================================
        // CLASIFICACIÓN Y NORMALIZACIÓN DEL MOVIMIENTO
        // ========================================================

        if ("deposito".equals(tipo)) {

            // ----------------------------------------------------
            // DEPÓSITO
            // ----------------------------------------------------
            // Todo depósito representa un ingreso.
            // Se normaliza el monto a positivo aunque el archivo
            // legacy lo entregue con signo negativo.

            totalDepositos = item.monto().abs();

            saldoMovimiento = totalDepositos;

        } else {

            // ----------------------------------------------------
            // RETIRO / COMPRA
            // ----------------------------------------------------
            // Todo retiro o compra representa un egreso.
            // Se normaliza el monto a negativo para representar
            // correctamente su efecto sobre el saldo.

            totalRetiros = item.monto().abs();

            saldoMovimiento =
                    totalRetiros.negate();
        }

        // ========================================================
        // RESULTADO PROCESADO
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