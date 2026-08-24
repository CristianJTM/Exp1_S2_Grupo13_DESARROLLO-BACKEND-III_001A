package com.bancoxyz.batch.config;


import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.infrastructure.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import java.math.BigDecimal;
import java.time.LocalDate;

@Configuration
public class BatchDataConfig {

    private static final String DATA_PATH = "data/semana_1/";


    // ============================================================
    // INPUT: TRANSACCIONES
    // ============================================================

    public record TransaccionInput(
            Long id,
            LocalDate fecha,
            BigDecimal monto,
            String tipo
    ) {
    }


    // ============================================================
    // RESULTADO PROCESADO: TRANSACCIONES
    // ============================================================

    public record TransaccionProcesada(
            Long id,
            LocalDate fecha,
            BigDecimal monto,
            String tipo,
            boolean anomalía,
            String observacion
    ) {
    }


    // ============================================================
    // INPUT: INTERESES
    // ============================================================

    public record InteresInput(
            Long cuentaId,
            String nombre,
            BigDecimal saldo,
            Integer edad,
            String tipo
    ) {
    }


    // ============================================================
    // RESULTADO PROCESADO: INTERESES
    // ============================================================

    public record InteresProcesado(
            Long cuentaId,
            String nombre,
            BigDecimal saldoInicial,
            Integer edad,
            String tipo,
            BigDecimal tasa,
            BigDecimal interes,
            BigDecimal saldoFinal
    ) {
    }


    // ============================================================
    // INPUT: CUENTAS ANUALES
    // ============================================================

    public record CuentaAnualInput(
            Long cuentaId,
            LocalDate fecha,
            String transaccion,
            BigDecimal monto,
            String descripcion
    ) {
    }


    // ============================================================
    // RESULTADO PROCESADO: CUENTAS ANUALES
    // ============================================================

    public record CuentaAnualProcesada(
            Long cuentaId,
            Integer anio,
            BigDecimal totalDepositos,
            BigDecimal totalRetiros,
            BigDecimal saldoMovimiento,
            Integer cantidadOperaciones,
            String observacion
    ) {
    }


    // ============================================================
    // READER BASE: TRANSACCIONES
    // ============================================================

    @Bean
    public FlatFileItemReader<TransaccionInput> transaccionesReaderBase() {

        return new FlatFileItemReaderBuilder<TransaccionInput>()
                .name("transaccionesReaderBase")
                .resource(
                        new FileSystemResource(
                                DATA_PATH + "transacciones.csv"
                        )
                )
                .linesToSkip(1)
                .delimited()
                .names(
                        "id",
                        "fecha",
                        "monto",
                        "tipo"
                )
                .fieldSetMapper(fieldSet -> {

                    LocalDate fecha = null;

                    String fechaTexto =
                            fieldSet.readString("fecha");

                    if (fechaTexto != null &&
                            !fechaTexto.isBlank()) {

                        try {
                            fecha = LocalDate.parse(
                                    fechaTexto
                            );
                        } catch (Exception ignored) {
                        }
                    }

                    BigDecimal monto = null;

                    String montoTexto =
                            fieldSet.readString("monto");

                    if (montoTexto != null &&
                            !montoTexto.isBlank()) {

                        try {
                            monto = new BigDecimal(
                                    montoTexto
                            );
                        } catch (Exception ignored) {
                        }
                    }

                    return new TransaccionInput(
                            fieldSet.readLong("id"),
                            fecha,
                            monto,
                            fieldSet.readString("tipo")
                    );
                })
                .build();
    }


    // ============================================================
    // READER SEGURO: TRANSACCIONES
    // ============================================================

    @Bean
    public SynchronizedItemStreamReader<TransaccionInput> transaccionesReader(
            FlatFileItemReader<TransaccionInput> transaccionesReaderBase) {

        return new SynchronizedItemStreamReaderBuilder<TransaccionInput>()
                .delegate(transaccionesReaderBase)
                .build();
    }


    // ============================================================
    // READER BASE: INTERESES
    // ============================================================

    @Bean
    public FlatFileItemReader<InteresInput> interesesReaderBase() {

        return new FlatFileItemReaderBuilder<InteresInput>()
                .name("interesesReaderBase")
                .resource(
                        new FileSystemResource(
                                DATA_PATH + "intereses.csv"
                        )
                )
                .linesToSkip(1)
                .delimited()
                .names(
                        "cuentaId",
                        "nombre",
                        "saldo",
                        "edad",
                        "tipo"
                )
                .fieldSetMapper(fieldSet -> {

                    BigDecimal saldo = null;

                    String saldoTexto =
                            fieldSet.readString("saldo");

                    if (saldoTexto != null &&
                            !saldoTexto.isBlank()) {

                        try {
                            saldo = new BigDecimal(
                                    saldoTexto
                            );
                        } catch (Exception ignored) {
                        }
                    }

                    Integer edad = null;

                    String edadTexto =
                            fieldSet.readString("edad");

                    if (edadTexto != null &&
                            !edadTexto.isBlank()) {

                        try {
                            edad = Integer.valueOf(
                                    edadTexto
                            );
                        } catch (Exception ignored) {
                        }
                    }

                    return new InteresInput(
                            fieldSet.readLong("cuentaId"),
                            fieldSet.readString("nombre"),
                            saldo,
                            edad,
                            fieldSet.readString("tipo")
                    );
                })
                .build();
    }


    // ============================================================
    // READER SEGURO: INTERESES
    // ============================================================

    @Bean
    public SynchronizedItemStreamReader<InteresInput> interesesReader(
            FlatFileItemReader<InteresInput> interesesReaderBase) {

        return new SynchronizedItemStreamReaderBuilder<InteresInput>()
                .delegate(interesesReaderBase)
                .build();
    }


    // ============================================================
    // READER BASE: CUENTAS ANUALES
    // ============================================================

    @Bean
    public FlatFileItemReader<CuentaAnualInput> cuentasAnualesReaderBase() {

        return new FlatFileItemReaderBuilder<CuentaAnualInput>()
                .name("cuentasAnualesReaderBase")
                .resource(
                        new FileSystemResource(
                                DATA_PATH + "cuentas_anuales.csv"
                        )
                )
                .linesToSkip(1)
                .delimited()
                .names(
                        "cuentaId",
                        "fecha",
                        "transaccion",
                        "monto",
                        "descripcion"
                )
                .fieldSetMapper(fieldSet -> {

                    LocalDate fecha = null;

                    String fechaTexto =
                            fieldSet.readString("fecha");

                    if (fechaTexto != null &&
                            !fechaTexto.isBlank()) {

                        try {
                            fecha = LocalDate.parse(
                                    fechaTexto
                            );
                        } catch (Exception ignored) {
                        }
                    }

                    BigDecimal monto = null;

                    String montoTexto =
                            fieldSet.readString("monto");

                    if (montoTexto != null &&
                            !montoTexto.isBlank()) {

                        try {
                            monto = new BigDecimal(
                                    montoTexto
                            );
                        } catch (Exception ignored) {
                        }
                    }

                    return new CuentaAnualInput(
                            fieldSet.readLong("cuentaId"),
                            fecha,
                            fieldSet.readString("transaccion"),
                            monto,
                            fieldSet.readString("descripcion")
                    );
                })
                .build();
    }


    // ============================================================
    // READER SEGURO: CUENTAS ANUALES
    // ============================================================

    @Bean
    public SynchronizedItemStreamReader<CuentaAnualInput> cuentasAnualesReader(
            FlatFileItemReader<CuentaAnualInput> cuentasAnualesReaderBase) {

        return new SynchronizedItemStreamReaderBuilder<CuentaAnualInput>()
                .delegate(cuentasAnualesReaderBase)
                .build();
    }
}