package com.bancoxyz.batch.config;

import com.bancoxyz.batch.config.BatchDataConfig.InteresInput;
import com.bancoxyz.batch.config.BatchDataConfig.InteresProcesado;
import com.bancoxyz.batch.exception.DatoInvalidoException;
import com.bancoxyz.batch.listener.BatchJobListener;
import com.bancoxyz.batch.listener.InteresSkipListener;
import com.bancoxyz.batch.processor.InteresProcessor;
import com.bancoxyz.batch.writer.InteresWriter;

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
public class InteresesJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    private final SynchronizedItemStreamReader<InteresInput> interesesReader;
    private final InteresProcessor interesProcessor;
    private final InteresWriter interesWriter;
    private final BatchJobListener batchJobListener;
    private final InteresSkipListener interesSkipListener;
    private final ThreadPoolTaskExecutor batchTaskExecutor;

    public InteresesJobConfig(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            SynchronizedItemStreamReader<InteresInput> interesesReader,
            InteresProcessor interesProcessor,
            InteresWriter interesWriter,
            BatchJobListener batchJobListener,
            InteresSkipListener interesSkipListener,
            ThreadPoolTaskExecutor batchTaskExecutor) {

        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.interesesReader = interesesReader;
        this.interesProcessor = interesProcessor;
        this.interesWriter = interesWriter;
        this.batchJobListener = batchJobListener;
        this.interesSkipListener = interesSkipListener;
        this.batchTaskExecutor = batchTaskExecutor;
    }

    @Bean
    public Job interesesJob() {

        return new JobBuilder(
                "interesesJob",
                jobRepository
        )
                .listener(batchJobListener)
                .start(interesesStep())
                .build();
    }

    @Bean
    public Step interesesStep() {

        return new StepBuilder(
                "interesesStep",
                jobRepository
        )
                .<InteresInput, InteresProcesado>chunk(5)

                .transactionManager(transactionManager)
                .reader(interesesReader)
                .processor(interesProcessor)
                .writer(interesWriter)

                .taskExecutor(batchTaskExecutor)

                .faultTolerant()
                .listener(interesSkipListener)
                /*
                 * Los errores de acceso a datos pueden ser
                 * transitorios, por lo que se permiten
                 * hasta 3 reintentos.
                 */
                .retry(DataAccessException.class)
                .retryLimit(3)
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

                .skipLimit(650)

                .build();
    }
}