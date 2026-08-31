package com.bancoxyz.batch.listener;

import com.bancoxyz.batch.config.BatchDataConfig.InteresInput;
import com.bancoxyz.batch.config.BatchDataConfig.InteresProcesado;
import com.bancoxyz.batch.model.AnomaliaTransaccion;
import com.bancoxyz.batch.repository.AnomaliaTransaccionRepository;

import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class InteresSkipListener
        implements SkipListener<InteresInput, InteresProcesado> {

    private final AnomaliaTransaccionRepository anomaliaRepository;

    public InteresSkipListener(
            AnomaliaTransaccionRepository anomaliaRepository) {

        this.anomaliaRepository = anomaliaRepository;
    }

    // ========================================================
    // REGISTRO OMITIDO DURANTE LA LECTURA
    // ========================================================

    @Override
    public void onSkipInRead(Throwable throwable) {

        System.out.println(
                "REGISTRO DE INTERÉS OMITIDO DURANTE LA LECTURA -> "
                        + throwable.getMessage()
        );

        if (throwable instanceof FlatFileParseException) {

            AnomaliaTransaccion anomalia =
                    new AnomaliaTransaccion(
                            null,
                            null,
                            "ERROR_LECTURA_INTERESES",
                            "Error de formato en archivo CSV de intereses: "
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
            InteresInput item,
            Throwable throwable) {

        String descripcion =
                throwable.getMessage() != null
                        ? throwable.getMessage()
                        : "Error desconocido durante el procesamiento.";

        System.out.println(
                "CUENTA "
                        + item.cuentaId()
                        + " -> Registro de interés omitido: "
                        + descripcion
        );

        AnomaliaTransaccion anomalia =
                new AnomaliaTransaccion(
                        item.cuentaId(),
                        null,
                        "DATO_INVALIDO_INTERES",
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
            InteresProcesado item,
            Throwable throwable) {

        String descripcion =
                throwable.getMessage() != null
                        ? throwable.getMessage()
                        : "Error durante la escritura en la base de datos.";

        System.out.println(
                "CUENTA "
                        + item.cuentaId()
                        + " -> Error de escritura: "
                        + descripcion
        );

        AnomaliaTransaccion anomalia =
                new AnomaliaTransaccion(
                        item.cuentaId(),
                        null,
                        "ERROR_ESCRITURA_INTERES",
                        descripcion,
                        LocalDateTime.now()
                );

        anomaliaRepository.save(anomalia);
    }
}