package com.bancoxyz.batch.repository;

import com.bancoxyz.batch.model.ResumenTransacciones;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.Optional;

@Repository
public interface ResumenTransaccionesRepository
        extends JpaRepository<ResumenTransacciones, Long> {

    Optional<ResumenTransacciones> findByFecha(LocalDate fecha);
}
