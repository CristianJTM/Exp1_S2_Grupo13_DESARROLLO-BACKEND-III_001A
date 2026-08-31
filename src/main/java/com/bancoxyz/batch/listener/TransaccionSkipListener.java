package com.bancoxyz.batch.listener;

import com.bancoxyz.batch.config.BatchDataConfig.TransaccionInput;
import com.bancoxyz.batch.config.BatchDataConfig.TransaccionProcesada;
import com.bancoxyz.batch.model.AnomaliaTransaccion;
import com.bancoxyz.batch.repository.AnomaliaTransaccionRepository;

import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class TransaccionSkipListener
        implements SkipListener<TransaccionInput, TransaccionProcesada> {

    private final AnomaliaTransaccionRepository anomaliaRepository;

    public TransaccionSkipListener(
            AnomaliaTransaccionRepository anomaliaRepository) {

        this.anomaliaRepository = anomaliaRepository;
    }

    // ========================================================
    // REGISTRO OMITIDO DURANTE LA LECTURA
    // ========================================================

    @Override
    public void onSkipInRead(Throwable throwable) {

        System.out.println(
                "REGISTRO OMITIDO DURANTE LA LECTURA -> "
                        + throwable.getMessage()
        );

        if (throwable instanceof FlatFileParseException) {

            AnomaliaTransaccion anomalia =
                    new AnomaliaTransaccion(
                            null,
                            null,
                            "ERROR_LECTURA",
                            "Error de formato en archivo CSV: "
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
            TransaccionInput item,
            Throwable throwable) {

        String descripcion =
                throwable.getMessage() != null
                        ? throwable.getMessage()
                        : "Error desconocido durante el procesamiento.";

        System.out.println(
                "TRANSACCIÓN "
                        + item.id()
                        + " -> Registro omitido: "
                        + descripcion
        );

        AnomaliaTransaccion anomalia =
                new AnomaliaTransaccion(
                        item.id(),
                        item.fecha(),
                        "DATO_INVALIDO",
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
            TransaccionProcesada item,
            Throwable throwable) {

        String descripcion =
                throwable.getMessage() != null
                        ? throwable.getMessage()
                        : "Error durante la escritura en la base de datos.";

        System.out.println(
                "TRANSACCIÓN "
                        + item.id()
                        + " -> Error de escritura: "
                        + descripcion
        );

        AnomaliaTransaccion anomalia =
                new AnomaliaTransaccion(
                        item.id(),
                        item.fecha(),
                        "ERROR_ESCRITURA",
                        descripcion,
                        LocalDateTime.now()
                );

        anomaliaRepository.save(anomalia);
    }
}