package com.bancoxyz.batch.writer;

import com.bancoxyz.batch.config.BatchDataConfig.TransaccionProcesada;
import com.bancoxyz.batch.model.Transaccion;
import com.bancoxyz.batch.repository.TransaccionRepository;


import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;

@Component
public class TransaccionWriter
        implements ItemWriter<TransaccionProcesada> {

    private final TransaccionRepository transaccionRepository;

    public TransaccionWriter(
            TransaccionRepository transaccionRepository) {

        this.transaccionRepository = transaccionRepository;
    }

    @Override
    public void write(
            Chunk<? extends TransaccionProcesada> chunk) {

        for (TransaccionProcesada item : chunk.getItems()) {

            Transaccion transaccion = new Transaccion();

            transaccion.setId(item.id());
            transaccion.setFecha(item.fecha());
            transaccion.setMonto(item.monto());
            transaccion.setTipo(item.tipo());
            transaccion.setObservacion(item.observacion());

            transaccionRepository.save(transaccion);
        }
    }
}
