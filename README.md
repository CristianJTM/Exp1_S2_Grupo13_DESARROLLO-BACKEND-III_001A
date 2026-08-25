# Banco XYZ - Procesamiento Batch

Aplicación desarrollada con **Java 21, Spring Boot y Spring Batch** para modernizar los procesos batch de un sistema legacy bancario.

---

## Tabla de contenidos

1. [Descripción del proyecto](#1-descripción-del-proyecto)
2. [Objetivo](#2-objetivo)
3. [Tecnologías utilizadas](#3-tecnologías-utilizadas)
4. [Estructura del proyecto](#4-estructura-del-proyecto)
5. [Arquitectura del procesamiento](#5-arquitectura-del-procesamiento)
6. [Configuración de datos](#6-configuración-de-datos)
7. [Procesamiento de transacciones](#7-procesamiento-de-transacciones)
8. [Cálculo de intereses](#8-cálculo-de-intereses)
9. [Generación de estados de cuenta anuales](#9-generación-de-estados-de-cuenta-anuales)
10. [Resumen de anomalías](#10-resumen-de-anomalías)
11. [Validaciones y manejo de errores](#11-validaciones-y-manejo-de-errores)
12. [Tolerancia a fallos y reintentos](#12-tolerancia-a-fallos-y-reintentos)
13. [Procesamiento paralelo y escalamiento](#13-procesamiento-paralelo-y-escalamiento)
14. [Listener](#14-listener)
15. [Jobs implementados](#15-jobs-implementados)
16. [Job orquestador](#16-job-orquestador)
17. [Persistencia](#17-persistencia)
18. [Base de datos](#18-base-de-datos)
19. [Configuración de Spring Batch](#19-configuración-de-spring-batch)
20. [Instalación y ejecución](#20-instalación-y-ejecución)
21. [Estrategia de procesamiento](#21-estrategia-de-procesamiento)
22. [Resultados esperados](#22-resultados-esperados)

---

## 1. Descripción del proyecto

El proyecto **Banco XYZ Batch** corresponde a una aplicación desarrollada con **Java 21, Spring Boot y Spring Batch**, cuyo objetivo es modernizar procesos batch de un sistema legacy bancario.

La solución permite procesar información bancaria proveniente de archivos CSV, aplicar validaciones y transformaciones mediante componentes de Spring Batch y almacenar los resultados procesados en una base de datos relacional MySQL.

El sistema implementa tres procesos principales:

- Reporte de transacciones diarias.
- Cálculo de intereses mensuales.
- Generación de estados de cuenta anuales.

Además, se implementa un Job orquestador denominado `procesoBatchCompleto`, que permite ejecutar los procesos de manera secuencial.

La solución incorpora mecanismos de:

- Procesamiento por chunks.
- Ejecución paralela mediante 3 hilos.
- Tolerancia a fallos.
- Reintentos (`retry`).
- Omisión controlada de registros (`skip`).
- Validación de datos.
- Persistencia mediante Spring Data JPA.
- Listener para controlar el inicio y finalización de los Jobs.
- Logs para identificar los hilos utilizados y medir la ejecución de los Steps.

---

## 2. Objetivo

Implementar una solución batch moderna que permita reemplazar procesos tradicionales del sistema legacy del Banco XYZ por una arquitectura basada en Spring Batch.

El procesamiento sigue el modelo:

```
Archivos CSV
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

Cada proceso utiliza el patrón **Reader → Processor → Writer**.

Los datos son leídos desde archivos CSV, validados y transformados mediante un `ItemProcessor` y finalmente almacenados en tablas MySQL.

El sistema también utiliza un `TaskExecutor` para distribuir el procesamiento en tres hilos utilizando chunks de tamaño 5.

---

## 3. Tecnologías utilizadas

| Tecnología | Uso |
|---|---|
| Java 21 | Lenguaje de programación |
| Spring Boot 4.1.0 | Framework principal |
| Spring Batch 6.0.4 | Procesamiento batch |
| Spring Data JPA | Persistencia de datos |
| Hibernate | ORM |
| MySQL 8.4 | Base de datos |
| Maven | Gestión y construcción del proyecto |
| Git / GitHub | Control de versiones y entrega |

> **Nota sobre versiones:** Spring Boot 4.1.0 gestiona directamente la versión compatible de Spring Batch mediante su BOM. En el `pom.xml` no se deben declarar versiones manuales para `spring-batch-core` ni `spring-batch-infrastructure`; basta con utilizar el starter correspondiente de Spring Boot.

---

## 4. Estructura del proyecto

```
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
│   │   │   └── com/bancoxyz/batch/
│   │   │       │
│   │   │       ├── BancoXyzBatchApplication.java
│   │   │       │
│   │   │       ├── config/
│   │   │       │   ├── BatchDataConfig.java
│   │   │       │   ├── BatchTaskExecutorConfig.java
│   │   │       │   ├── EstadosAnualesJobConfig.java
│   │   │       │   ├── InteresesJobConfig.java
│   │   │       │   ├── ProcesoBatchCompletoJobConfig.java
│   │   │       │   └── TransaccionesJobConfig.java
│   │   │       │
│   │   │       ├── listener/
│   │   │       │   └── BatchJobListener.java
│   │   │       │
│   │   │       ├── model/
│   │   │       │   ├── CuentaAnual.java
│   │   │       │   ├── Interes.java
│   │   │       │   ├── ResumenTransacciones.java
│   │   │       │   └── Transaccion.java
│   │   │       │
│   │   │       ├── processor/
│   │   │       │   ├── CuentaAnualProcessor.java
│   │   │       │   ├── InteresProcessor.java
│   │   │       │   ├── ResumenTransaccionesProcessor.java
│   │   │       │   └── TransaccionProcessor.java
│   │   │       │
│   │   │       ├── repository/
│   │   │       │   ├── CuentaRepository.java
│   │   │       │   ├── EstadoCuentaRepository.java
│   │   │       │   ├── ResumenTransaccionesRepository.java
│   │   │       │   └── TransaccionRepository.java
│   │   │       │
│   │   │       ├── tasklet/
│   │   │       │   └── ResumenAnomaliasTasklet.java
│   │   │       │
│   │   │       └── writer/
│   │   │           ├── CuentaAnualWriter.java
│   │   │           ├── InteresWriter.java
│   │   │           ├── ResumenTransaccionesWriter.java
│   │   │           └── TransaccionWriter.java
│   │   │
│   │   └── resources/
│   │       └── application.properties
│   │
│   └── test/
│       └── java/
│           └── com/bancoxyz/batch/
│               └── BancoXyzBatchApplicationTests.java
```

La carpeta `data/` se encuentra en la raíz del proyecto y contiene los archivos CSV utilizados como entrada para los procesos batch.

---

## 5. Arquitectura del procesamiento

La aplicación utiliza una arquitectura basada en los componentes principales de Spring Batch:

```
                 ┌───────────────────────┐
                 │      Archivos CSV      │
                 └───────────┬───────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │      Reader     │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    Processor    │
                    │ Validaciones +  │
                    │ Transformación  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     Writer      │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   MySQL / JPA   │
                    └─────────────────┘
```

Los Steps utilizan procesamiento por chunks de tamaño 5:

```
CSV
 │
 ├── Chunk 1 → 5 registros
 │
 ├── Chunk 2 → 5 registros
 │
 ├── Chunk 3 → 5 registros
 │
 └── ...
```

Para mejorar el rendimiento, los chunks son procesados utilizando un pool de tres hilos:

```
                 TaskExecutor
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
 batch-thread-1  batch-thread-2  batch-thread-3
        │             │             │
        └─────────────┼─────────────┘
                      ▼
                Procesamiento
```

---

## 6. Configuración de datos

### 6.1 BatchDataConfig

`BatchDataConfig.java` contiene los modelos de entrada y salida utilizados por los procesos batch, además de los `FlatFileItemReader` encargados de leer los archivos CSV.

Los archivos utilizados son:

- `data/semana_1/transacciones.csv`
- `data/semana_1/intereses.csv`
- `data/semana_1/cuentas_anuales.csv`

Los Readers transforman los valores de texto provenientes de los CSV a tipos Java como:

- `Long`
- `Integer`
- `BigDecimal`
- `LocalDate`
- `String`

Cuando un valor numérico o una fecha no puede ser interpretado correctamente, el Reader permite que el valor quede como `null` para que posteriormente sea validado por el Processor.

---

## 7. Procesamiento de transacciones

### 7.1 TransaccionesJobConfig

`TransaccionesJobConfig.java` define:

```
transaccionesJob
        │
        ▼
transaccionesStep
        │
        ├── Reader
        ├── Processor
        └── Writer
```

El Step utiliza:

- `chunk(5)`
- `TaskExecutor` con 3 hilos.
- `faultTolerant()`
- `retry`
- `skip`
- Límite de reintentos.
- Límite de omisiones.

El proceso permite detectar transacciones que presentan anomalías y almacenar el resultado en la tabla:

```
transacciones_procesadas
```

---

## 8. Cálculo de intereses

### 8.1 InteresesJobConfig

`InteresesJobConfig.java` define el procesamiento de los datos contenidos en `intereses.csv`.

El flujo corresponde a:

```
interesesReader
       │
       ▼
InteresProcessor
       │
       ▼
InteresWriter
       │
       ▼
intereses_procesados
```

El `InteresProcessor` valida los datos de entrada y determina la tasa correspondiente según el tipo de cuenta.

Las tasas implementadas son:

| Tipo | Tasa |
|---|---|
| Ahorro | 2% |
| Préstamo | 5% |
| Hipoteca | 4% |

Posteriormente se calcula el interés y el saldo final.

Los resultados son almacenados en:

```
intereses_procesados
```

---

## 9. Generación de estados de cuenta anuales

### 9.1 EstadosAnualesJobConfig

`EstadosAnualesJobConfig.java` define el procesamiento de los movimientos contenidos en `cuentas_anuales.csv`.

El flujo corresponde a:

```
cuentasAnualesReader
       │
       ▼
CuentaAnualProcessor
       │
       ▼
CuentaAnualWriter
       │
       ▼
estados_anuales
```

El procesamiento permite:

- Validar el identificador de cuenta.
- Validar la fecha.
- Validar el monto.
- Validar la descripción.
- Validar el tipo de operación.
- Clasificar depósitos y retiros.
- Calcular el movimiento correspondiente.
- Determinar el año de la operación.
- Registrar la cantidad de operaciones procesadas.

Los resultados son almacenados en:

```
estados_anuales
```

---

## 10. Resumen de anomalías

Como parte del proceso de transacciones se implementó un Step adicional: `resumenAnomaliasStep`.

Este Step utiliza un Tasklet para consultar las transacciones procesadas y generar un resumen de las anomalías detectadas.

El flujo corresponde a:

```
transacciones_procesadas
          │
          ▼
ResumenAnomaliasTasklet
          │
          ▼
resumen_transacciones
```

El resumen contempla:

- Total de transacciones procesadas.
- Cantidad de transacciones válidas.
- Cantidad de transacciones con anomalías.
- Fecha del procesamiento.
- Observaciones asociadas a las anomalías.

Los resultados se almacenan en:

```
resumen_transacciones
```

De esta forma, el proceso de transacciones no solamente almacena cada registro procesado, sino que también genera un resumen general útil para revisión y auditoría.

---

## 11. Validaciones y manejo de errores

Los Processor implementan reglas de validación para evitar que información inconsistente sea almacenada.

**Transacciones**
- Identificación de datos inválidos.
- Validación del monto.
- Validación de fechas.
- Identificación de anomalías.
- Generación de observaciones.

**Intereses**
- Validación del saldo.
- Validación de edad.
- Validación del tipo de cuenta.
- Validación de los valores necesarios para calcular intereses.

**Estados anuales**
- Validación del identificador de cuenta.
- Validación de fecha.
- Validación del monto.
- Validación de descripción.
- Validación del tipo de operación.

Los registros que no cumplen las reglas de procesamiento pueden ser omitidos mediante la política de tolerancia a fallos de Spring Batch.

---

## 12. Tolerancia a fallos y reintentos

Los Steps principales utilizan configuración de tolerancia a fallos mediante `.faultTolerant()`.

La política implementada considera:

```
Retry
  └── Hasta 3 intentos

Skip
  └── Hasta 20 registros omitidos
```

Conceptualmente:

```
             Registro
                 │
                 ▼
            Procesamiento
                 │
          ┌──────┴──────┐
          │             │
       Correcto        Error
          │             │
          ▼             ▼
       Writer        Retry x3
                        │
                 ┌──────┴──────┐
                 │             │
             Recuperado     Continúa error
                 │             │
                 ▼             ▼
              Writer         Skip
                               │
                               ▼
                        Límite de 20
```

La configuración permite que un error individual no provoque automáticamente la interrupción de todo el procesamiento.

Si se supera el límite de omisiones configurado, el Step puede finalizar con error, evitando que el sistema continúe procesando datos cuando la cantidad de registros problemáticos supera el nivel permitido.

---

## 13. Procesamiento paralelo y escalamiento

La aplicación implementa procesamiento paralelo mediante un `ThreadPoolTaskExecutor` definido en `BatchTaskExecutorConfig.java`.

La configuración establece:

| Parámetro | Valor |
|---|---|
| Core threads | 3 |
| Maximum threads | 3 |
| Queue capacity | 10 |
| Thread prefix | `batch-thread-` |

Los Steps utilizan este executor mediante `.taskExecutor(batchTaskExecutor)`.

El procesamiento se realiza utilizando chunks de tamaño 5 mediante `.chunk(5)`.

Por lo tanto, la configuración cumple con el requisito de utilizar **3 hilos de ejecución paralela** + **chunks de 5 registros**.

Los nombres de los hilos permiten identificar el procesamiento en los logs:

- `batch-thread-1`
- `batch-thread-2`
- `batch-thread-3`

Esto facilita el análisis del comportamiento y distribución del procesamiento.

---

## 14. Listener

`BatchJobListener.java` implementa `JobExecutionListener`.

El listener permite controlar eventos asociados a la ejecución de los Jobs y registrar:

- Inicio del Job.
- Nombre del Job.
- Identificador de ejecución.
- Estado final.
- Exit Status.
- Hora de inicio.
- Hora de finalización.
- Excepciones asociadas a una ejecución fallida.

Cuando el Job finaliza correctamente, se informa: `El Job finalizó correctamente.`

En caso de error, se informa: `El Job finalizó con errores.`

Esto permite disponer de información adicional para monitoreo y auditoría del procesamiento.

---

## 15. Jobs implementados

### 15.1 Job de Transacciones

```
transaccionesJob
        │
        ▼
transaccionesStep
        │
        ├── transaccionesReader
        ├── TransaccionProcessor
        └── TransaccionWriter
        │
        ▼
transacciones_procesadas
        │
        ▼
resumenAnomaliasStep
        │
        ▼
resumen_transacciones
```

### 15.2 Job de Intereses

```
interesesJob
        │
        ▼
interesesStep
        │
        ├── interesesReader
        ├── InteresProcessor
        └── InteresWriter
        │
        ▼
intereses_procesados
```

### 15.3 Job de Estados Anuales

```
estadosAnualesJob
        │
        ▼
estadosAnualesStep
        │
        ├── cuentasAnualesReader
        ├── CuentaAnualProcessor
        └── CuentaAnualWriter
        │
        ▼
estados_anuales
```

---

## 16. Job orquestador

`ProcesoBatchCompletoJobConfig.java` define el Job `procesoBatchCompleto`.

Este Job reutiliza los Steps existentes y los ejecuta secuencialmente:

```
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

De esta forma, cada Step mantiene su propia configuración, mientras que el Job orquestador permite ejecutar el flujo completo.

La utilización de un Job orquestador evita duplicar la lógica de procesamiento y permite mantener una arquitectura modular.

---

## 17. Persistencia

La aplicación utiliza Spring Data JPA y Hibernate para almacenar los resultados en MySQL.

Las principales tablas utilizadas son:

- `transacciones_procesadas`
- `intereses_procesados`
- `estados_anuales`
- `resumen_transacciones`

Los componentes de persistencia se encuentran en `com.bancoxyz.batch.repository`.

Los Writers utilizan estos repositorios para almacenar los objetos generados por los Processors.

---

## 18. Base de datos

La aplicación utiliza MySQL como sistema gestor de base de datos.

La configuración se encuentra en `src/main/resources/application.properties`.

Ejemplo de configuración:

```properties
spring.datasource.url=jdbc:mysql://localhost:3307/banco_xyz?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true
spring.datasource.username=banco_user
spring.datasource.password=banco_password
spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
```

Hibernate utiliza:

```properties
spring.jpa.hibernate.ddl-auto=update
```

Esto permite actualizar las estructuras de las tablas correspondientes a las entidades utilizadas por la aplicación.

Spring Batch también utiliza tablas internas para administrar la metadata de los Jobs y Steps:

```properties
spring.batch.jdbc.initialize-schema=always
```

---

## 19. Configuración de Spring Batch

La aplicación utiliza:

```properties
spring.batch.jdbc.initialize-schema=always
spring.batch.job.enabled=true
spring.batch.job.name=procesoBatchCompleto
```

La propiedad `spring.batch.job.name=procesoBatchCompleto` establece el Job que se ejecutará automáticamente al iniciar la aplicación.

Debido a que existen varios Jobs dentro del contexto de Spring, se especifica explícitamente el Job principal para evitar ambigüedades durante el arranque.

---

## 20. Instalación y ejecución

### Requisitos

Para ejecutar el proyecto se requiere:

- Java 21.
- Maven o Maven Wrapper.
- MySQL 8.x.
- Base de datos `banco_xyz`.
- Archivos CSV ubicados en la carpeta correspondiente dentro de `data/`.

Para la ejecución actual se utilizan los datos de `data/semana_1/`.

### 20.1 Compilación

Desde la raíz del proyecto ejecutar:

```bash
.\mvnw.cmd clean compile
```

Una compilación correcta debe finalizar con:

```
BUILD SUCCESS
```

### 20.2 Ejecución

Para iniciar la aplicación:

```bash
.\mvnw.cmd spring-boot:run
```

La configuración actual ejecutará automáticamente `procesoBatchCompleto`.

El flujo será:

```
Transacciones
      ↓
Intereses
      ↓
Estados Anuales
      ↓
Resumen de Anomalías
      ↓
Job COMPLETED
```

### 20.3 Ejecutar un Job individual

También es posible ejecutar los Jobs de manera independiente.

**Transacciones**
```bash
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.arguments=--spring.batch.job.name=transaccionesJob"
```

**Intereses**
```bash
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.arguments=--spring.batch.job.name=interesesJob"
```

**Estados Anuales**
```bash
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.arguments=--spring.batch.job.name=estadosAnualesJob"
```

**Proceso completo**
```bash
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.arguments=--spring.batch.job.name=procesoBatchCompleto"
```

Los nombres de Job disponibles son:

- `transaccionesJob`
- `interesesJob`
- `estadosAnualesJob`
- `procesoBatchCompleto`

---

## 21. Estrategia de procesamiento

La solución implementa una estrategia orientada a mantener la integridad de los datos y permitir la continuidad del procesamiento ante errores.

El flujo general es:

```
             CSV
              │
              ▼
            Reader
              │
              ▼
          Validación
              │
              ▼
          Processor
              │
       ┌──────┴──────┐
       │             │
    Correcto       Error
       │             │
       ▼             ▼
     Writer       Retry
       │             │
       │       ┌─────┴─────┐
       │       │           │
       │   Recuperado     Error
       │       │           │
       │       ▼           ▼
       │     Writer       Skip
       │
       ▼
    MySQL
```

El uso combinado de:

- `chunk(5)`
- 3 hilos
- `faultTolerant()`
- `retryLimit(3)`
- `skipLimit(20)`
- Listeners
- Logs

permite construir un procesamiento batch orientado a la eficiencia, continuidad y control de errores.

---

## 22. Resultados esperados

Al finalizar correctamente el procesamiento, el sistema debe:

- Procesar las transacciones provenientes del CSV.
- Detectar y registrar anomalías.
- Generar un resumen de anomalías.
- Calcular los intereses correspondientes.
- Generar los estados de cuenta anuales.
- Persistir los resultados en MySQL.
- Ejecutar los Steps utilizando chunks de tamaño 5.
- Utilizar hasta 3 hilos de procesamiento paralelo.
- Aplicar las políticas de retry y skip configuradas.
- Registrar el inicio y finalización del Job mediante el listener.
- Finalizar el Job con estado `COMPLETED` cuando el procesamiento sea exitoso.
