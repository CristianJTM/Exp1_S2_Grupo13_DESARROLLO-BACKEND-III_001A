package com.bancoxyz.batch.repository;

import com.bancoxyz.batch.model.AnomaliaTransaccion;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

@Repository
public interface AnomaliaTransaccionRepository
        extends JpaRepository<AnomaliaTransaccion, Long> {

    List<AnomaliaTransaccion> findByFechaTransaccion(
            LocalDate fechaTransaccion);
}
