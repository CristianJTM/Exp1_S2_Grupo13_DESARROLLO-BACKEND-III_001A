# Banco XYZ - Procesamiento Batch

Aplicación desarrollada con **Java 21, Spring Boot 4.1.0 y Spring Batch 6.0.4** para modernizar y automatizar procesos batch de un sistema legacy bancario.

La solución permite procesar información proveniente de archivos CSV, aplicar reglas de validación y transformación, detectar anomalías, generar resúmenes y persistir los resultados en una base de datos **MySQL 8.4**.

---

## Tabla de contenidos

1. [Descripción del proyecto](#1-descripción-del-proyecto)
2. [Objetivo](#2-objetivo)
3. [Tecnologías utilizadas](#3-tecnologías-utilizadas)
4. [Estructura del proyecto](#4-estructura-del-proyecto)
5. [Arquitectura general](#5-arquitectura-general)
6. [Configuración de datos de entrada](#6-configuración-de-datos-de-entrada)
7. [Procesamiento de transacciones](#7-procesamiento-de-transacciones)
8. [Procesamiento de intereses](#8-procesamiento-de-intereses)
9. [Procesamiento de estados de cuenta anuales](#9-procesamiento-de-estados-de-cuenta-anuales)
10. [Gestión de anomalías](#10-gestión-de-anomalías)
11. [Resumen de transacciones](#11-resumen-de-transacciones)
12. [Validaciones y manejo de errores](#12-validaciones-y-manejo-de-errores)
13. [Tolerancia a fallos y reintentos](#13-tolerancia-a-fallos-y-reintentos)
14. [Procesamiento paralelo](#14-procesamiento-paralelo)
15. [Listeners](#15-listeners)
16. [Jobs implementados](#16-jobs-implementados)
17. [Job orquestador](#17-job-orquestador)
18. [Servicio de ejecución](#18-servicio-de-ejecución)
19. [Controlador REST](#19-controlador-rest)
20. [Persistencia](#20-persistencia)
21. [Base de datos](#21-base-de-datos)
22. [Configuración de Spring Batch](#22-configuración-de-spring-batch)
23. [Datos de prueba](#23-datos-de-prueba)
24. [Instalación y ejecución](#24-instalación-y-ejecución)
25. [Estrategia general de procesamiento](#25-estrategia-general-de-procesamiento)
26. [Resultados esperados](#26-resultados-esperados)

---

## 1. Descripción del proyecto

El proyecto **Banco XYZ Batch** corresponde a una aplicación desarrollada con **Java 21, Spring Boot y Spring Batch**, cuyo propósito es modernizar procesos batch pertenecientes a un sistema legacy bancario.

La aplicación recibe información desde archivos CSV y ejecuta diferentes procesos de negocio utilizando la arquitectura de procesamiento proporcionada por Spring Batch.

Los procesos principales implementados son:

* Procesamiento de transacciones.
* Cálculo de intereses.
* Generación de estados de cuenta anuales.
* Detección y registro de anomalías.
* Generación de resúmenes de transacciones.

La aplicación utiliza una arquitectura basada en:

```text
CSV
 │
 ▼
Reader
 │
 ▼
Processor
 │
 ▼
Writer
 │
 ▼
Base de datos MySQL
```

Cada proceso permite separar las responsabilidades de lectura, validación, transformación y persistencia.

La solución incorpora además:

* Procesamiento por chunks.
* Ejecución paralela mediante `TaskExecutor`.
* Manejo de registros inválidos.
* Omisión controlada mediante `skip`.
* Reintentos mediante `retry`.
* Registro de anomalías.
* Resúmenes de procesamiento.
* Persistencia mediante Spring Data JPA.
* Hibernate como ORM.
* Listeners para monitoreo de Jobs.
* Servicio para ejecutar los procesos batch.
* Controlador REST para iniciar el procesamiento.
* Job orquestador para ejecutar el flujo completo.

---

## 2. Objetivo

El objetivo del proyecto es implementar una solución batch moderna para reemplazar procesos tradicionales de un sistema bancario legacy.

La aplicación debe permitir procesar grandes cantidades de información de manera estructurada, controlada y tolerante a errores.

El flujo principal es:

```text
                  ARCHIVOS CSV
                       │
          ┌────────────┼────────────┐
          │            │            │
          ▼            ▼            ▼
    Transacciones   Intereses   Cuentas anuales
          │            │            │
          ▼            ▼            ▼
      Processor     Processor    Processor
          │            │            │
          ▼            ▼            ▼
       Writer       Writer       Writer
          │            │            │
          └────────────┼────────────┘
                       ▼
                    MySQL
```

Para el procesamiento completo se dispone además de un Job orquestador:

```text
procesoBatchCompleto
        │
        ▼
Transacciones
        │
        ▼
Intereses
        │
        ▼
Estados Anuales
        │
        ▼
Resumen de Transacciones
```

---

## 3. Tecnologías utilizadas

| Tecnología          | Uso                        |
| ------------------- | -------------------------- |
| Java 21             | Lenguaje de programación   |
| Spring Boot 4.1.0   | Framework principal        |
| Spring Batch 6.0.4  | Procesamiento batch        |
| Spring Data JPA     | Persistencia               |
| Hibernate ORM 7.4.1 | Mapeo objeto-relacional    |
| MySQL 8.4           | Base de datos              |
| Maven               | Gestión y construcción     |
| Git                 | Control de versiones       |
| GitHub              | Repositorio y entrega      |
| Docker              | Contenedorización de MySQL |

La aplicación utiliza Maven Wrapper, por lo que no es necesario tener Maven instalado globalmente para ejecutar el proyecto.

---

## 4. Estructura del proyecto

La estructura actual del proyecto es:

```text
BancoXyzBatch/
│
├── .gitattributes
├── .gitignore
├── docker-compose.yaml
├── Evidencias.docx
├── HELP.md
├── mvnw
├── mvnw.cmd
├── pom.xml
├── README.md
│
├── .idea/
│
├── .mvn/
│   └── wrapper/
│       └── maven-wrapper.properties
│
├── data/
│   ├── semana_1/
│   │   ├── cuentas_anuales.csv
│   │   ├── intereses.csv
│   │   └── transacciones.csv
│   │
│   ├── semana_2/
│   │   ├── cuentas_anuales.csv
│   │   ├── intereses.csv
│   │   └── transacciones.csv
│   │
│   └── semana_3/
│       ├── cuentas_anuales.csv
│       ├── intereses.csv
│       └── transacciones.csv
│
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/
│   │   │       └── bancoxyz/
│   │   │           └── batch/
│   │   │
│   │   │               ├── BancoXyzBatchApplication.java
│   │   │               │
│   │   │               ├── config/
│   │   │               │   ├── BatchDataConfig.java
│   │   │               │   ├── BatchTaskExecutorConfig.java
│   │   │               │   ├── EstadosAnualesJobConfig.java
│   │   │               │   ├── InteresesJobConfig.java
│   │   │               │   ├── ProcesoBatchCompletoJobConfig.java
│   │   │               │   └── TransaccionesJobConfig.java
│   │   │               │
│   │   │               ├── controller/
│   │   │               │   └── BatchJobController.java
│   │   │               │
│   │   │               ├── exception/
│   │   │               │   ├── BatchJobLaunchException.java
│   │   │               │   └── DatoInvalidoException.java
│   │   │               │
│   │   │               ├── listener/
│   │   │               │   ├── BatchJobListener.java
│   │   │               │   ├── CuentaAnualSkipListener.java
│   │   │               │   ├── InteresSkipListener.java
│   │   │               │   └── TransaccionSkipListener.java
│   │   │               │
│   │   │               ├── model/
│   │   │               │   ├── AnomaliaTransaccion.java
│   │   │               │   ├── CuentaAnual.java
│   │   │               │   ├── Interes.java
│   │   │               │   ├── ResumenTransacciones.java
│   │   │               │   └── Transaccion.java
│   │   │               │
│   │   │               ├── processor/
│   │   │               │   ├── CuentaAnualProcessor.java
│   │   │               │   ├── InteresProcessor.java
│   │   │               │   ├── ResumenTransaccionesProcessor.java
│   │   │               │   └── TransaccionProcessor.java
│   │   │               │
│   │   │               ├── repository/
│   │   │               │   ├── AnomaliaTransaccionRepository.java
│   │   │               │   ├── CuentaRepository.java
│   │   │               │   ├── EstadoCuentaRepository.java
│   │   │               │   ├── ResumenTransaccionesRepository.java
│   │   │               │   └── TransaccionRepository.java
│   │   │               │
│   │   │               ├── service/
│   │   │               │   └── BatchJobService.java
│   │   │               │
│   │   │               ├── tasklet/
│   │   │               │   └── ResumenAnomaliasTasklet.java
│   │   │               │
│   │   │               └── writer/
│   │   │                   ├── CuentaAnualWriter.java
│   │   │                   ├── InteresWriter.java
│   │   │                   ├── ResumenTransaccionesWriter.java
│   │   │                   └── TransaccionWriter.java
│   │   │
│   │   └── resources/
│   │       └── application.properties
│   │
│   └── test/
│       └── java/
│           └── com/
│               └── bancoxyz/
│                   └── batch/
│                       └── BancoXyzBatchApplicationTests.java
```

### Organización por responsabilidad

La aplicación utiliza una estructura modular:

```text
config
 └── Configuración de Jobs, Steps y procesamiento

controller
 └── Endpoints REST

exception
 └── Excepciones personalizadas

listener
 └── Monitoreo de Jobs y registros omitidos

model
 └── Entidades y modelos de datos

processor
 └── Validación y transformación

repository
 └── Acceso a datos mediante JPA

service
 └── Ejecución y orquestación de Jobs

tasklet
 └── Procesamientos específicos

writer
 └── Persistencia de resultados
```

---

## 5. Arquitectura general

La arquitectura de la aplicación está basada en Spring Batch y utiliza diferentes componentes para separar cada responsabilidad.

```text
                         ┌──────────────────────┐
                         │      Cliente REST    │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │ BatchJobController   │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │   BatchJobService    │
                         └──────────┬───────────┘
                                    │
                                    ▼
                       ┌─────────────────────────┐
                       │ procesoBatchCompleto    │
                       └────────────┬────────────┘
                                    │
                 ┌──────────────────┼──────────────────┐
                 ▼                  ▼                  ▼
          Transacciones          Intereses       Estados Anuales
                 │                  │                  │
                 ▼                  ▼                  ▼
             Reader             Reader             Reader
                 │                  │                  │
                 ▼                  ▼                  ▼
             Processor           Processor           Processor
                 │                  │                  │
                 ▼                  ▼                  ▼
              Writer             Writer             Writer
                 │                  │                  │
                 └──────────────────┼──────────────────┘
                                    ▼
                                  MySQL
```

La arquitectura también permite ejecutar Jobs de manera individual.

---

## 6. Configuración de datos de entrada

Los archivos de entrada se encuentran en la carpeta `data/`.

Actualmente se dispone de tres conjuntos de datos:

```text
data/
├── semana_1/
├── semana_2/
└── semana_3/
```

Cada carpeta contiene:

```text
cuentas_anuales.csv
intereses.csv
transacciones.csv
```

Esto permite utilizar diferentes conjuntos de datos para realizar pruebas y comparar el comportamiento del procesamiento.

### BatchDataConfig

`BatchDataConfig.java` concentra la configuración relacionada con los datos de entrada y los Readers utilizados por los Jobs.

Los Readers se encargan de transformar los datos provenientes de los CSV en objetos Java.

Dependiendo del archivo, se utilizan tipos como:

* `Long`
* `Integer`
* `BigDecimal`
* `LocalDate`
* `String`

Los datos posteriormente son enviados al Processor correspondiente.

---

## 7. Procesamiento de transacciones

El procesamiento de transacciones está definido en `TransaccionesJobConfig.java`.

El flujo principal es:

```text
transacciones.csv
       │
       ▼
transaccionesReader
       │
       ▼
TransaccionProcessor
       │
       ├── Validación
       ├── Detección de anomalías
       └── Transformación
       │
       ▼
TransaccionWriter
       │
       ├───────────────┐
       ▼               ▼
transacciones_     anomalías
procesadas
```

El `TransaccionProcessor` valida los registros provenientes del archivo.

Entre las situaciones detectadas se encuentran:

* Fecha inexistente.
* Tipo de transacción inválido.
* Datos incompletos.
* Valores que no cumplen las reglas definidas.
* Otras inconsistencias de los datos de entrada.

Por ejemplo, durante las pruebas se identificaron registros como:

```text
Transacción 7: fecha inexistente.
Transacción 9: tipo de transacción inválido: invalid
Transacción 10: tipo de transacción inválido: invalid
```

Los registros que presentan errores pueden ser omitidos mediante la configuración de tolerancia a fallos y quedan registrados mediante el `TransaccionSkipListener`.

---

## 8. Procesamiento de intereses

El procesamiento de intereses se encuentra definido en:

```text
InteresesJobConfig.java
```

El flujo es:

```text
intereses.csv
      │
      ▼
interesesReader
      │
      ▼
InteresProcessor
      │
      ├── Validación
      ├── Determinación de tasa
      └── Cálculo
      │
      ▼
InteresWriter
      │
      ▼
intereses_procesados
```

El `InteresProcessor` valida los datos y realiza el cálculo correspondiente según el tipo de cuenta.

Las tasas utilizadas son:

| Tipo de cuenta | Tasa |
| -------------- | ---: |
| Ahorro         |   2% |
| Préstamo       |   5% |
| Hipoteca       |   4% |

El resultado procesado es almacenado mediante `InteresWriter`.

Los registros que presentan errores pueden ser omitidos y registrados mediante `InteresSkipListener`.

---

## 9. Procesamiento de estados de cuenta anuales

El procesamiento de estados anuales se encuentra definido en:

```text
EstadosAnualesJobConfig.java
```

El flujo es:

```text
cuentas_anuales.csv
        │
        ▼
cuentasAnualesReader
        │
        ▼
CuentaAnualProcessor
        │
        ├── Validación
        ├── Clasificación
        └── Transformación
        │
        ▼
CuentaAnualWriter
        │
        ▼
estados_anuales
```

El Processor permite validar información como:

* Identificador de cuenta.
* Fecha.
* Monto.
* Descripción.
* Tipo de operación.

También permite clasificar los movimientos correspondientes a depósitos y retiros y determinar información asociada al período de la operación.

Los registros que no cumplen las reglas establecidas pueden ser omitidos mediante la política de `skip`.

---

## 10. Gestión de anomalías

Una de las modificaciones importantes incorporadas al proyecto corresponde a la separación de las anomalías respecto de la entidad principal `Transaccion`.

Para esto se implementó la entidad:

```text
AnomaliaTransaccion.java
```

junto con:

```text
AnomaliaTransaccionRepository.java
```

Esto permite representar las anomalías como información independiente y mantener un registro específico de los problemas encontrados durante el procesamiento.

La arquitectura conceptual es:

```text
                  Transacción
                       │
                       ▼
              TransaccionProcessor
                       │
              ┌────────┴────────┐
              │                 │
              ▼                 ▼
          Válida            Anómala
              │                 │
              ▼                 ▼
           Writer          AnomaliaTransaccion
                                │
                                ▼
                  AnomaliaTransaccionRepository
```

Las anomalías pueden incluir información como:

* Identificador de la transacción.
* Fecha de la transacción cuando está disponible.
* Motivo de la anomalía.
* Información asociada al registro original.

Esta separación permite evitar que una transacción inválida impida el funcionamiento general del proceso.

---

## 11. Resumen de transacciones

El proyecto incorpora el modelo:

```text
ResumenTransacciones.java
```

junto con:

```text
ResumenTransaccionesProcessor.java
ResumenTransaccionesWriter.java
ResumenTransaccionesRepository.java
```

Estos componentes permiten generar información consolidada del procesamiento de transacciones.

El flujo general es:

```text
Transacciones procesadas
          │
          ▼
ResumenTransaccionesProcessor
          │
          ▼
ResumenTransaccionesWriter
          │
          ▼
resumen_transacciones
```

El resumen permite disponer de información general sobre los resultados obtenidos durante la ejecución.

Adicionalmente, se dispone de:

```text
ResumenAnomaliasTasklet.java
```

para realizar un procesamiento específico relacionado con el resumen de anomalías.

La separación entre el procesamiento individual y el resumen permite mantener responsabilidades independientes dentro de la arquitectura.

---

## 12. Validaciones y manejo de errores

Los Processors son responsables de validar los datos antes de enviarlos a los Writers.

### Transacciones

Se consideran, entre otras, las siguientes validaciones:

* Identificador de transacción.
* Fecha.
* Tipo de transacción.
* Monto.
* Consistencia de los datos.

### Intereses

Se consideran:

* Identificación de la cuenta.
* Saldo.
* Edad u otros datos requeridos.
* Tipo de cuenta.
* Información necesaria para el cálculo.

### Estados anuales

Se consideran:

* Identificador de cuenta.
* Fecha.
* Monto.
* Descripción.
* Tipo de operación.

Cuando un registro no cumple las condiciones requeridas, el procesamiento puede lanzar una excepción controlada.

Para esto se dispone de excepciones personalizadas:

```text
DatoInvalidoException
BatchJobLaunchException
```

Esto permite diferenciar los errores relacionados con los datos de aquellos asociados al lanzamiento de los Jobs.

---

## 13. Tolerancia a fallos y reintentos

Los Jobs utilizan las capacidades de tolerancia a fallos proporcionadas por Spring Batch.

La configuración contempla mecanismos de:

```text
Retry
Skip
Listeners
```

El flujo conceptual es:

```text
                 Registro
                    │
                    ▼
               Processor
                    │
             ┌──────┴──────┐
             │             │
          Correcto        Error
             │             │
             ▼             ▼
           Writer        Retry
                           │
                    ┌──────┴──────┐
                    │             │
                Recuperado      Error
                    │             │
                    ▼             ▼
                  Writer         Skip
                                   │
                                   ▼
                             SkipListener
```

El mecanismo `retry` permite volver a intentar determinadas operaciones cuando corresponde.

El mecanismo `skip` permite omitir registros problemáticos sin detener inmediatamente todo el Job.

Los listeners específicos permiten registrar información sobre los elementos omitidos:

```text
CuentaAnualSkipListener
InteresSkipListener
TransaccionSkipListener
```

Esto proporciona trazabilidad sobre los registros que no pudieron ser procesados normalmente.

---

## 14. Procesamiento paralelo

El proyecto utiliza:

```text
BatchTaskExecutorConfig.java
```

para configurar el executor utilizado por los Steps.

La configuración permite distribuir el procesamiento utilizando varios hilos.

La arquitectura conceptual es:

```text
                    TaskExecutor
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
   batch-thread-1  batch-thread-2  batch-thread-3
          │              │              │
          └──────────────┼──────────────┘
                         ▼
                  Procesamiento
```

Los Steps utilizan procesamiento por chunks.

La configuración utilizada permite procesar los registros en grupos, reduciendo el costo de realizar operaciones individuales para cada elemento.

Durante la ejecución, los nombres de los hilos permiten identificar el procesamiento en los logs.

---

## 15. Listeners

El proyecto utiliza distintos listeners para monitorear la ejecución.

### BatchJobListener

`BatchJobListener.java` implementa el seguimiento de la ejecución de los Jobs.

Permite registrar información como:

* Inicio del Job.
* Nombre del Job.
* Identificador de ejecución.
* Estado final.
* Exit Status.
* Hora de inicio.
* Hora de finalización.
* Información relacionada con errores.

Conceptualmente:

```text
Job
 │
 ├── beforeJob()
 │
 ▼
Procesamiento
 │
 └── afterJob()
        │
        ▼
   Estado final
```

Esto facilita la supervisión y auditoría de las ejecuciones.

### Skip Listeners

También existen listeners especializados:

```text
CuentaAnualSkipListener
InteresSkipListener
TransaccionSkipListener
```

Su función es registrar información de los elementos que son omitidos durante el procesamiento.

---

## 16. Jobs implementados

La aplicación cuenta con cuatro Jobs principales.

### 16.1 transaccionesJob

```text
transaccionesJob
       │
       ▼
transaccionesStep
       │
       ├── Reader
       ├── Processor
       └── Writer
       │
       ▼
Transacciones procesadas
       │
       ▼
Anomalías / Resumen
```

---

### 16.2 interesesJob

```text
interesesJob
      │
      ▼
interesesStep
      │
      ├── Reader
      ├── Processor
      └── Writer
      │
      ▼
intereses_procesados
```

---

### 16.3 estadosAnualesJob

```text
estadosAnualesJob
        │
        ▼
estadosAnualesStep
        │
        ├── Reader
        ├── Processor
        └── Writer
        │
        ▼
estados_anuales
```

---

### 16.4 procesoBatchCompleto

```text
procesoBatchCompleto
        │
        ▼
transaccionesStep
        │
        ▼
interesesStep
        │
        ▼
estadosAnualesStep
        │
        ▼
resumenAnomaliasStep
```

Este Job permite ejecutar el procesamiento completo de forma centralizada.

---

## 17. Job orquestador

`ProcesoBatchCompletoJobConfig.java` implementa el Job:

```text
procesoBatchCompleto
```

Su función es reutilizar los Steps previamente configurados y ejecutar el flujo general.

La idea principal es evitar duplicar la lógica de cada proceso.

```text
                  procesoBatchCompleto
                           │
                           ▼
                  transaccionesStep
                           │
                           ▼
                    interesesStep
                           │
                           ▼
                  estadosAnualesStep
                           │
                           ▼
                 resumenAnomaliasStep
```

Cada Step conserva su responsabilidad individual, mientras que el Job orquestador define el orden general de ejecución.

---

## 18. Servicio de ejecución

La lógica de lanzamiento de Jobs se encuentra centralizada en:

```text
BatchJobService.java
```

El servicio actúa como capa intermedia entre el controlador y Spring Batch.

Conceptualmente:

```text
Controller
    │
    ▼
BatchJobService
    │
    ▼
JobLauncher / JobOperator
    │
    ▼
Spring Batch
```

Esta separación permite evitar que el controlador contenga directamente la lógica de ejecución de los Jobs.

El servicio también permite centralizar el tratamiento de errores asociados al lanzamiento de una ejecución batch.

---

## 19. Controlador REST

La aplicación dispone de:

```text
BatchJobController.java
```

Este componente permite iniciar el procesamiento mediante una interfaz HTTP.

El controlador recibe la solicitud y delega la ejecución al:

```text
BatchJobService
```

El flujo general es:

```text
Solicitud HTTP
      │
      ▼
BatchJobController
      │
      ▼
BatchJobService
      │
      ▼
Job
      │
      ▼
Spring Batch
```

Esto permite integrar el procesamiento batch con otros sistemas o realizar pruebas mediante herramientas como Postman o un cliente HTTP.

---

## 20. Persistencia

La persistencia se implementa mediante:

```text
Spring Data JPA
        +
Hibernate
        +
MySQL
```

Los repositorios se encuentran en:

```text
com.bancoxyz.batch.repository
```

Los principales repositorios son:

```text
AnomaliaTransaccionRepository
CuentaRepository
EstadoCuentaRepository
ResumenTransaccionesRepository
TransaccionRepository
```

Los Writers utilizan estos repositorios para almacenar la información procesada.

La separación entre `model`, `repository` y `writer` permite mantener una arquitectura organizada:

```text
Processor
    │
    ▼
Writer
    │
    ▼
Repository
    │
    ▼
Hibernate / JPA
    │
    ▼
MySQL
```

---

## 21. Base de datos

La aplicación utiliza **MySQL 8.4** como sistema gestor de base de datos.

La conexión se encuentra configurada en:

```text
src/main/resources/application.properties
```

La URL utilizada durante el desarrollo es:

```text
jdbc:mysql://localhost:3307/banco_xyz?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true
```

La aplicación utiliza el puerto `3307` del equipo host para conectarse al servicio MySQL.

### Tablas de negocio

Las principales entidades utilizadas por la aplicación corresponden a los resultados procesados, incluyendo:

```text
transacciones_procesadas
intereses_procesados
estados_anuales
anomalias_transaccion
resumen_transacciones
```

Además, Spring Batch utiliza sus propias tablas de metadata para administrar:

* Jobs.
* Job Executions.
* Steps.
* Step Executions.
* Parámetros.
* Estados de ejecución.

---

## 22. Configuración de Spring Batch

Spring Batch requiere tablas internas para mantener la información de ejecución.

La configuración contempla:

```properties
spring.batch.jdbc.initialize-schema=always
```

Esto permite inicializar las tablas de metadata de Spring Batch.

La aplicación también utiliza la configuración de ejecución de Jobs definida en:

```text
application.properties
```

La arquitectura permite trabajar con múltiples Jobs dentro del mismo contexto:

```text
transaccionesJob
interesesJob
estadosAnualesJob
procesoBatchCompleto
```

El Job completo constituye el flujo principal para ejecutar todos los procesos de forma integrada.

---

## 23. Datos de prueba

El proyecto incluye tres conjuntos de datos:

```text
data/semana_1/
data/semana_2/
data/semana_3/
```

Cada conjunto contiene:

```text
cuentas_anuales.csv
intereses.csv
transacciones.csv
```

La separación por semanas permite trabajar con distintos escenarios de prueba.

Durante las pruebas del procesamiento de transacciones se utilizaron registros con datos inválidos para verificar el funcionamiento de las validaciones, el mecanismo `skip` y los listeners.

Por ejemplo:

```text
Transacción 7
→ fecha inexistente

Transacción 9
→ tipo de transacción inválido

Transacción 10
→ tipo de transacción inválido
```

Estos casos permiten comprobar que el sistema puede identificar y registrar datos problemáticos sin detener necesariamente el procesamiento completo.

---

## 24. Instalación y ejecución

### 24.1 Requisitos

Para ejecutar el proyecto se requiere:

* Java 21.
* Docker.
* Docker Compose.
* Git.
* MySQL 8.4, ya sea mediante Docker o una instalación local.
* Sistema operativo Windows, Linux o macOS.

El proyecto incluye Maven Wrapper, por lo que no es necesario instalar Maven manualmente.

---

### 24.2 Iniciar MySQL mediante Docker

Desde la raíz del proyecto:

```bash
docker compose up -d
```

Esto inicia los servicios definidos en:

```text
docker-compose.yaml
```

Se puede comprobar el estado de los contenedores mediante:

```bash
docker ps
```

---

### 24.3 Compilar el proyecto

En Windows:

```bash
.\mvnw.cmd clean compile
```

En Linux/macOS:

```bash
./mvnw clean compile
```

Una compilación correcta debe finalizar con:

```text
BUILD SUCCESS
```

---

### 24.4 Ejecutar las pruebas

Windows:

```bash
.\mvnw.cmd test
```

Linux/macOS:

```bash
./mvnw test
```

---

### 24.5 Iniciar la aplicación

Windows:

```bash
.\mvnw.cmd spring-boot:run
```

Linux/macOS:

```bash
./mvnw spring-boot:run
```

Una vez iniciada la aplicación, Spring Boot levanta el servidor web y deja disponible el controlador REST.

---

### 24.6 Ejecutar el proceso completo

El flujo completo corresponde a:

```text
procesoBatchCompleto
        │
        ▼
Transacciones
        │
        ▼
Intereses
        │
        ▼
Estados Anuales
        │
        ▼
Resumen
```

El Job puede ser iniciado desde el mecanismo configurado por la aplicación o mediante el controlador REST.

---

### 24.7 Ejecutar Jobs individuales

También es posible ejecutar cada Job de manera independiente cuando se requiere probar solamente un proceso.

#### Transacciones

```text
transaccionesJob
```

#### Intereses

```text
interesesJob
```

#### Estados anuales

```text
estadosAnualesJob
```

#### Proceso completo

```text
procesoBatchCompleto
```

Los Jobs disponibles son:

```text
transaccionesJob
interesesJob
estadosAnualesJob
procesoBatchCompleto
```

---

## 25. Estrategia general de procesamiento

La estrategia implementada combina procesamiento por chunks, validación, tolerancia a errores, persistencia y monitoreo.

El flujo general es:

```text
                       CSV
                        │
                        ▼
                     Reader
                        │
                        ▼
                    Processor
                        │
                ┌───────┴───────┐
                │               │
             Válido           Inválido
                │               │
                ▼               ▼
              Writer          Retry
                │               │
                │        ┌──────┴──────┐
                │        │             │
                │    Recuperado      Error
                │        │             │
                │        ▼             ▼
                │      Writer         Skip
                │                      │
                │                      ▼
                │                 SkipListener
                │
                ▼
              MySQL
```

En el caso de las transacciones se agrega el tratamiento específico de anomalías:

```text
Transacción
     │
     ▼
Processor
     │
 ┌───┴────┐
 │        │
 ▼        ▼
OK      Anomalía
 │        │
 ▼        ▼
Writer   AnomaliaTransaccion
 │        │
 │        ▼
 │     Repository
 │
 ▼
MySQL
```

La arquitectura permite mantener la continuidad del procesamiento y conservar información sobre los registros problemáticos.

---

## 26. Resultados esperados

Al finalizar correctamente el procesamiento, el sistema debe ser capaz de:

* Leer información desde archivos CSV.
* Procesar transacciones bancarias.
* Validar los datos de entrada.
* Detectar transacciones anómalas.
* Registrar las anomalías identificadas.
* Omitir registros inválidos de forma controlada.
* Registrar los elementos omitidos mediante Skip Listeners.
* Calcular intereses según el tipo de cuenta.
* Generar estados de cuenta anuales.
* Generar resúmenes de transacciones.
* Persistir los resultados en MySQL.
* Ejecutar los procesos mediante Spring Batch.
* Utilizar procesamiento por chunks.
* Utilizar procesamiento paralelo mediante `TaskExecutor`.
* Aplicar mecanismos de `retry` y `skip`.
* Registrar el inicio y finalización de los Jobs mediante listeners.
* Permitir el lanzamiento de procesos mediante el servicio y controlador REST.
* Ejecutar los Jobs individualmente.
* Ejecutar el flujo completo mediante `procesoBatchCompleto`.
* Mantener información de ejecución mediante las tablas internas de Spring Batch.

El resultado final corresponde a una solución batch modular, tolerante a errores y orientada a la modernización de los procesos legacy del Banco XYZ.

---

## Flujo completo de la solución

```text
                         ┌──────────────────┐
                         │   Archivos CSV   │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │     Readers      │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │    Processors    │
                         │                  │
                         │ Validación       │
                         │ Transformación   │
                         │ Anomalías        │
                         └────────┬─────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
                    ▼                           ▼
              Datos válidos               Datos inválidos
                    │                           │
                    ▼                           ▼
                 Writers                   Retry / Skip
                    │                           │
                    │                     Skip Listeners
                    │
                    ▼
                 MySQL
                    │
          ┌─────────┼──────────┐
          │         │          │
          ▼         ▼          ▼
   Transacciones  Intereses  Estados
          │
          ▼
     Anomalías
          │
          ▼
      Resúmenes
```

---

## Licencia

Proyecto académico desarrollado para la asignatura **Desarrollo Backend III**, orientado a la implementación de procesos batch utilizando Spring Boot, Spring Batch, Spring Data JPA y MySQL.
