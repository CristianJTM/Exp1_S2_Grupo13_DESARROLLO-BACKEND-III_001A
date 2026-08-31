package com.bancoxyz.batch.model;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "anomalias_transaccion")
public class AnomaliaTransaccion {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "transaccion_id")
    private Long transaccionId;

    @Column(nullable = false)
    private String tipo;

    @Column(nullable = false, length = 500)
    private String descripcion;

    @Column(name = "fecha_registro", nullable = false)
    private LocalDateTime fechaRegistro;

    public AnomaliaTransaccion() {
    }

    public AnomaliaTransaccion(
            Long transaccionId,
            String tipo,
            String descripcion,
            LocalDateTime fechaRegistro) {

        this.transaccionId = transaccionId;
        this.tipo = tipo;
        this.descripcion = descripcion;
        this.fechaRegistro = fechaRegistro;
    }

    public Long getId() {
        return id;
    }

    public Long getTransaccionId() {
        return transaccionId;
    }

    public String getTipo() {
        return tipo;
    }

    public String getDescripcion() {
        return descripcion;
    }

    public LocalDateTime getFechaRegistro() {
        return fechaRegistro;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setTransaccionId(Long transaccionId) {
        this.transaccionId = transaccionId;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public void setDescripcion(String descripcion) {
        this.descripcion = descripcion;
    }

    public void setFechaRegistro(LocalDateTime fechaRegistro) {
        this.fechaRegistro = fechaRegistro;
    }
}