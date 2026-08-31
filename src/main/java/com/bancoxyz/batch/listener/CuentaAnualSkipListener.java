package com.bancoxyz.batch.listener;

import com.bancoxyz.batch.config.BatchDataConfig.CuentaAnualInput;
import com.bancoxyz.batch.config.BatchDataConfig.CuentaAnualProcesada;
import com.bancoxyz.batch.model.AnomaliaTransaccion;
import com.bancoxyz.batch.repository.AnomaliaTransaccionRepository;

import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class CuentaAnualSkipListener
        implements SkipListener<CuentaAnualInput, CuentaAnualProcesada> {

    private final AnomaliaTransaccionRepository anomaliaRepository;

    public CuentaAnualSkipListener(
            AnomaliaTransaccionRepository anomaliaRepository) {

        this.anomaliaRepository = anomaliaRepository;
    }

    // ========================================================
    // REGISTRO OMITIDO DURANTE LA LECTURA
    // ========================================================

    @Override
    public void onSkipInRead(Throwable throwable) {

        System.out.println(
                "REGISTRO DE CUENTA ANUAL OMITIDO DURANTE LA LECTURA -> "
                        + throwable.getMessage()
        );

        if (throwable instanceof FlatFileParseException) {

            AnomaliaTransaccion anomalia =
                    new AnomaliaTransaccion(
                            null,
                            null,
                            "ERROR_LECTURA_CUENTA_ANUAL",
                            "Error de formato en archivo CSV de cuentas anuales: "
                                    + throwable.getMessage(),
                            LocalDateTime.now()
                    );

            anomaliaRepository.save(anomalia);
        }
    }

    // ========================================================
    // REGISTRO OMITIDO DURANTE EL PROCESAMIENTO
    // ========================================================

    @Override
    public void onSkipInProcess(
            CuentaAnualInput item,
            Throwable throwable) {

        String descripcion =
                throwable.getMessage() != null
                        ? throwable.getMessage()
                        : "Error desconocido durante el procesamiento.";

        System.out.println(
                "CUENTA ANUAL "
                        + item.cuentaId()
                        + " -> Registro omitido: "
                        + descripcion
        );

        AnomaliaTransaccion anomalia =
                new AnomaliaTransaccion(
                        item.cuentaId(),
                        item.fecha(),
                        "DATO_INVALIDO_CUENTA_ANUAL",
                        descripcion,
                        LocalDateTime.now()
                );

        anomaliaRepository.save(anomalia);
    }

    // ========================================================
    // REGISTRO OMITIDO DURANTE LA ESCRITURA
    // ========================================================

    @Override
    public void onSkipInWrite(
            CuentaAnualProcesada item,
            Throwable throwable) {

        String descripcion =
                throwable.getMessage() != null
                        ? throwable.getMessage()
                        : "Error durante la escritura en la base de datos.";

        System.out.println(
                "CUENTA ANUAL "
                        + item.cuentaId()
                        + " -> Error de escritura: "
                        + descripcion
        );

        AnomaliaTransaccion anomalia =
                new AnomaliaTransaccion(
                        item.cuentaId(),
                        null,
                        "ERROR_ESCRITURA_CUENTA_ANUAL",
                        descripcion,
                        LocalDateTime.now()
                );

        anomaliaRepository.save(anomalia);
    }
}