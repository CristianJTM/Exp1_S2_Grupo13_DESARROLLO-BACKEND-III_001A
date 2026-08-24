package com.bancoxyz.batch.model;

import jakarta.persistence.*;

import java.time.LocalDate;

@Entity
@Table(name = "resumen_transacciones")
public class ResumenTransacciones {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "fecha", nullable = false)
    private LocalDate fecha;

    @Column(name = "observacion", length = 500)
    private String observacion;

    @Column(name = "total_transacciones", nullable = false)
    private Integer totalTransacciones;

    @Column(name = "transacciones_anomalas", nullable = false)
    private Integer transaccionesAnomalas;

    @Column(name = "transacciones_validas", nullable = false)
    private Integer transaccionesValidas;

    public ResumenTransacciones() {
    }

    public ResumenTransacciones(
            LocalDate fecha,
            String observacion,
            Integer totalTransacciones,
            Integer transaccionesAnomalas,
            Integer transaccionesValidas) {

        this.fecha = fecha;
        this.observacion = observacion;
        this.totalTransacciones = totalTransacciones;
        this.transaccionesAnomalas = transaccionesAnomalas;
        this.transaccionesValidas = transaccionesValidas;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public LocalDate getFecha() {
        return fecha;
    }

    public void setFecha(LocalDate fecha) {
        this.fecha = fecha;
    }

    public String getObservacion() {
        return observacion;
    }

    public void setObservacion(String observacion) {
        this.observacion = observacion;
    }

    public Integer getTotalTransacciones() {
        return totalTransacciones;
    }

    public void setTotalTransacciones(Integer totalTransacciones) {
        this.totalTransacciones = totalTransacciones;
    }

    public Integer getTransaccionesAnomalas() {
        return transaccionesAnomalas;
    }

    public void setTransaccionesAnomalas(Integer transaccionesAnomalas) {
        this.transaccionesAnomalas = transaccionesAnomalas;
    }

    public Integer getTransaccionesValidas() {
        return transaccionesValidas;
    }

    public void setTransaccionesValidas(Integer transaccionesValidas) {
        this.transaccionesValidas = transaccionesValidas;
    }
}