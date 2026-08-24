package com.bancoxyz.batch.config;

import com.bancoxyz.batch.config.BatchDataConfig.InteresInput;
import com.bancoxyz.batch.config.BatchDataConfig.InteresProcesado;
import com.bancoxyz.batch.listener.BatchJobListener;
import com.bancoxyz.batch.processor.InteresProcessor;
import com.bancoxyz.batch.writer.InteresWriter;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.support.SynchronizedItemStreamReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class InteresesJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    private final FlatFileItemReader<InteresInput> interesesReader;
    private final InteresProcessor interesProcessor;
    private final InteresWriter interesWriter;
    private final BatchJobListener batchJobListener;
    private final ThreadPoolTaskExecutor batchTaskExecutor;

    public InteresesJobConfig(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<InteresInput> interesesReader,
            InteresProcessor interesProcessor,
            InteresWriter interesWriter,
            BatchJobListener batchJobListener,
            ThreadPoolTaskExecutor batchTaskExecutor) {

        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.interesesReader = interesesReader;
        this.interesProcessor = interesProcessor;
        this.interesWriter = interesWriter;
        this.batchJobListener = batchJobListener;
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

                // Procesamiento paralelo con 3 hilos
                .taskExecutor(batchTaskExecutor)

                // Tolerancia a fallos
                .faultTolerant()
                .retry(Exception.class)
                .retryLimit(3)
                .skip(Exception.class)
                .skipLimit(20)

                .build();
    }
}