package com.bancoxyz.batch.writer;

import com.bancoxyz.batch.model.ResumenTransacciones;
import com.bancoxyz.batch.repository.ResumenTransaccionesRepository;

import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;

@Component
public class ResumenTransaccionesWriter
        implements ItemWriter<ResumenTransacciones> {

    private final ResumenTransaccionesRepository
            resumenRepository;

    public ResumenTransaccionesWriter(
            ResumenTransaccionesRepository resumenRepository) {

        this.resumenRepository =
                resumenRepository;
    }

    @Override
    public void write(
            Chunk<? extends ResumenTransacciones> chunk) {

        for (ResumenTransacciones item :
                chunk.getItems()) {

            resumenRepository
                    .findByFecha(item.getFecha())
                    .ifPresentOrElse(
                            existente -> {

                                existente.setTotalTransacciones(
                                        item.getTotalTransacciones()
                                );

                                existente.setTransaccionesValidas(
                                        item.getTransaccionesValidas()
                                );

                                existente.setTransaccionesAnomalas(
                                        item.getTransaccionesAnomalas()
                                );

                                existente.setObservacion(
                                        item.getObservacion()
                                );

                                resumenRepository.save(
                                        existente
                                );
                            },

                            () -> resumenRepository.save(item)
                    );
        }
    }
}
