package com.bancoxyz.batch.writer;

import com.bancoxyz.batch.config.BatchDataConfig.CuentaAnualProcesada;
import com.bancoxyz.batch.model.CuentaAnual;
import com.bancoxyz.batch.repository.EstadoCuentaRepository;


import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

@Component
public class CuentaAnualWriter
        implements ItemWriter<CuentaAnualProcesada> {

    private final EstadoCuentaRepository estadoCuentaRepository;

    public CuentaAnualWriter(
            EstadoCuentaRepository estadoCuentaRepository) {

        this.estadoCuentaRepository =
                estadoCuentaRepository;
    }

    @Override
    public void write(
            Chunk<? extends CuentaAnualProcesada> chunk) {

        for (CuentaAnualProcesada item : chunk.getItems()) {

            /*
             * Busca si ya existe un estado consolidado
             * para la misma cuenta y año.
             */
            List<CuentaAnual> existentes =
                    estadoCuentaRepository.findByCuentaIdAndAnio(
                            item.cuentaId(),
                            item.anio()
                    );

            CuentaAnual cuentaAnual;

            if (!existentes.isEmpty()) {

                /*
                 * Ya existe un registro para la cuenta y año.
                 * Se actualiza acumulando los valores.
                 */
                cuentaAnual = existentes.get(0);

                BigDecimal totalDepositos =
                        cuentaAnual.getTotalDepositos() != null
                                ? cuentaAnual.getTotalDepositos()
                                : BigDecimal.ZERO;

                BigDecimal totalRetiros =
                        cuentaAnual.getTotalRetiros() != null
                                ? cuentaAnual.getTotalRetiros()
                                : BigDecimal.ZERO;

                BigDecimal saldoMovimiento =
                        cuentaAnual.getSaldoMovimiento() != null
                                ? cuentaAnual.getSaldoMovimiento()
                                : BigDecimal.ZERO;

                Integer cantidadOperaciones =
                        cuentaAnual.getCantidadOperaciones() != null
                                ? cuentaAnual.getCantidadOperaciones()
                                : 0;

                cuentaAnual.setTotalDepositos(
                        totalDepositos.add(
                                item.totalDepositos()
                        )
                );

                cuentaAnual.setTotalRetiros(
                        totalRetiros.add(
                                item.totalRetiros()
                        )
                );

                cuentaAnual.setSaldoMovimiento(
                        saldoMovimiento.add(
                                item.saldoMovimiento()
                        )
                );

                cuentaAnual.setCantidadOperaciones(
                        cantidadOperaciones
                                + item.cantidadOperaciones()
                );

                cuentaAnual.setObservacion(
                        "Estado de cuenta anual consolidado"
                );

            } else {

                /*
                 * No existe un registro para la cuenta y año,
                 * por lo tanto se crea el primer registro.
                 */
                cuentaAnual =
                        new CuentaAnual();

                cuentaAnual.setCuentaId(
                        item.cuentaId()
                );

                cuentaAnual.setAnio(
                        item.anio()
                );

                cuentaAnual.setTotalDepositos(
                        item.totalDepositos()
                );

                cuentaAnual.setTotalRetiros(
                        item.totalRetiros()
                );

                cuentaAnual.setSaldoMovimiento(
                        item.saldoMovimiento()
                );

                cuentaAnual.setCantidadOperaciones(
                        item.cantidadOperaciones()
                );

                cuentaAnual.setObservacion(
                        "Estado de cuenta anual consolidado"
                );
            }

            estadoCuentaRepository.save(
                    cuentaAnual
            );
        }
    }
}