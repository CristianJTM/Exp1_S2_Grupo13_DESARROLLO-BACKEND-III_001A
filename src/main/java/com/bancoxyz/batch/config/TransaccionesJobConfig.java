package com.bancoxyz.batch.config;

import com.bancoxyz.batch.config.BatchDataConfig.TransaccionInput;
import com.bancoxyz.batch.config.BatchDataConfig.TransaccionProcesada;
import com.bancoxyz.batch.listener.BatchJobListener;
import com.bancoxyz.batch.processor.TransaccionProcessor;
import com.bancoxyz.batch.tasklet.ResumenAnomaliasTasklet;
import com.bancoxyz.batch.writer.TransaccionWriter;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.batch.infrastructure.item.support.SynchronizedItemStreamReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class TransaccionesJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    private final SynchronizedItemStreamReader<TransaccionInput> transaccionesReader;
    private final TransaccionProcessor transaccionProcessor;
    private final TransaccionWriter transaccionWriter;
    private final BatchJobListener batchJobListener;
    private final ResumenAnomaliasTasklet resumenAnomaliasTasklet;
    private final ThreadPoolTaskExecutor batchTaskExecutor;

    public TransaccionesJobConfig(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            SynchronizedItemStreamReader<TransaccionInput> transaccionesReader,
            TransaccionProcessor transaccionProcessor,
            TransaccionWriter transaccionWriter,
            BatchJobListener batchJobListener,
            ResumenAnomaliasTasklet resumenAnomaliasTasklet,
            ThreadPoolTaskExecutor batchTaskExecutor) {

        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.transaccionesReader = transaccionesReader;
        this.transaccionProcessor = transaccionProcessor;
        this.transaccionWriter = transaccionWriter;
        this.batchJobListener = batchJobListener;
        this.resumenAnomaliasTasklet = resumenAnomaliasTasklet;
        this.batchTaskExecutor = batchTaskExecutor;
    }

    @Bean
    public Job transaccionesJob() {

        return new JobBuilder(
                "transaccionesJob",
                jobRepository
        )
                .listener(batchJobListener)

                // 1. Procesamiento de transacciones
                .start(transaccionesStep())

                // 2. Generación del resumen de anomalías
                .next(resumenAnomaliasStep())

                .build();
    }

    @Bean
    public Step transaccionesStep() {

        return new StepBuilder(
                "transaccionesStep",
                jobRepository
        )
                .<TransaccionInput, TransaccionProcesada>chunk(5)

                .transactionManager(transactionManager)

                // Reader protegido para procesamiento concurrente
                .reader(transaccionesReader)

                // Validación y transformación de datos
                .processor(transaccionProcessor)

                // Persistencia de resultados
                .writer(transaccionWriter)

                // Procesamiento paralelo con 3 hilos
                .taskExecutor(batchTaskExecutor)

                .faultTolerant()

                /*
                 * Los errores de acceso a datos pueden ser transitorios,
                 * por lo que se permite realizar hasta 3 reintentos.
                 */
                .retry(DataAccessException.class)
                .retryLimit(3)
                /*
                 * Los errores de formato o estructura del CSV se
                 * consideran registros que pueden omitirse para
                 * permitir la continuidad del procesamiento.
                 */
                .skip(FlatFileParseException.class)
                .skipLimit(20)

                .build();
    }

    @Bean
    public Step resumenAnomaliasStep() {

        return new StepBuilder(
                "resumenAnomaliasStep",
                jobRepository
        )
                .tasklet(
                        resumenAnomaliasTasklet,
                        transactionManager
                )
                .build();
    }
}