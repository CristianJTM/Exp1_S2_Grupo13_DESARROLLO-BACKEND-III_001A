package com.bancoxyz.batch.repository;
import com.bancoxyz.batch.model.Transaccion;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

@Repository
public interface TransaccionRepository
        extends JpaRepository<Transaccion, Long> {

    List<Transaccion> findByFecha(LocalDate fecha);

    List<Transaccion> findByTipo(String tipo);

    List<Transaccion> findByAnomaliaTrue();

    List<Transaccion> findByFechaBetween(
            LocalDate fechaInicio,
            LocalDate fechaFin
    );

    @Query("SELECT DISTINCT t.fecha FROM Transaccion t ORDER BY t.fecha")
    List<LocalDate> findFechasProcesadas();
}
