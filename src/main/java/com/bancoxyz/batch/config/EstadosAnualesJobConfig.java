package com.bancoxyz.batch.config;

import com.bancoxyz.batch.config.BatchDataConfig.CuentaAnualInput;
import com.bancoxyz.batch.config.BatchDataConfig.CuentaAnualProcesada;
import com.bancoxyz.batch.exception.DatoInvalidoException;
import com.bancoxyz.batch.listener.BatchJobListener;
import com.bancoxyz.batch.listener.CuentaAnualSkipListener;
import com.bancoxyz.batch.processor.CuentaAnualProcessor;
import com.bancoxyz.batch.writer.CuentaAnualWriter;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.batch.infrastructure.item.support.SynchronizedItemStreamReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class EstadosAnualesJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    private final SynchronizedItemStreamReader<CuentaAnualInput> cuentasAnualesReader;
    private final CuentaAnualProcessor cuentaAnualProcessor;
    private final CuentaAnualWriter cuentaAnualWriter;
    private final BatchJobListener batchJobListener;
    private final CuentaAnualSkipListener cuentaAnualSkipListener;
    private final ThreadPoolTaskExecutor batchTaskExecutor;

    public EstadosAnualesJobConfig(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            SynchronizedItemStreamReader<CuentaAnualInput> cuentasAnualesReader,
            CuentaAnualProcessor cuentaAnualProcessor,
            CuentaAnualWriter cuentaAnualWriter,
            BatchJobListener batchJobListener,
            CuentaAnualSkipListener cuentaAnualSkipListener,
            ThreadPoolTaskExecutor batchTaskExecutor) {

        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.cuentasAnualesReader = cuentasAnualesReader;
        this.cuentaAnualProcessor = cuentaAnualProcessor;
        this.cuentaAnualWriter = cuentaAnualWriter;
        this.batchJobListener = batchJobListener;
        this.cuentaAnualSkipListener = cuentaAnualSkipListener;
        this.batchTaskExecutor = batchTaskExecutor;
    }

    @Bean
    public Job estadosAnualesJob() {

        return new JobBuilder(
                "estadosAnualesJob",
                jobRepository
        )
                .listener(batchJobListener)
                .start(estadosAnualesStep())
                .build();
    }

    @Bean
    public Step estadosAnualesStep() {

        return new StepBuilder(
                "estadosAnualesStep",
                jobRepository
        )
                .<CuentaAnualInput, CuentaAnualProcesada>chunk(5)

                .transactionManager(transactionManager)

                // ====================================================
                // READER
                // ====================================================

                .reader(cuentasAnualesReader)

                // ====================================================
                // PROCESSOR
                // ====================================================

                .processor(cuentaAnualProcessor)

                // ====================================================
                // WRITER
                // ====================================================

                .writer(cuentaAnualWriter)

                // ====================================================
                // PROCESAMIENTO PARALELO
                // ====================================================

                .taskExecutor(batchTaskExecutor)

                // ====================================================
                // TOLERANCIA A FALLOS
                // ====================================================

                .faultTolerant()

                // Listener encargado de registrar registros omitidos
                .listener(cuentaAnualSkipListener)

                // ====================================================
                // RETRY
                // ====================================================

                /*
                 * Los errores de acceso a datos pueden ser
                 * transitorios, por lo que se permiten
                 * hasta 3 reintentos.
                 */
                .retry(DataAccessException.class)
                .retryLimit(3)

                // ====================================================
                // SKIP
                // ====================================================

                /*
                 * Los errores de formato del archivo CSV se
                 * consideran registros que pueden omitirse.
                 */
                .skip(FlatFileParseException.class)

                /*
                 * Los datos que no cumplen las reglas de negocio
                 * son registrados por el SkipListener y omitidos.
                 */
                .skip(DatoInvalidoException.class)

                .skipLimit(20)

                .build();
    }
}
