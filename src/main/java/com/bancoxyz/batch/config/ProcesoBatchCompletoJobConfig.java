package com.bancoxyz.batch.config;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProcesoBatchCompletoJobConfig {

    private final JobRepository jobRepository;

    public ProcesoBatchCompletoJobConfig(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    @Bean
    public Job procesoBatchCompleto(
            Step transaccionesStep,
            Step interesesStep,
            Step estadosAnualesStep,
            Step resumenAnomaliasStep) {

        return new JobBuilder("procesoBatchCompleto", jobRepository)
                .start(transaccionesStep)
                .next(interesesStep)
                .next(estadosAnualesStep)
                .next(resumenAnomaliasStep)
                .build();
    }
}
