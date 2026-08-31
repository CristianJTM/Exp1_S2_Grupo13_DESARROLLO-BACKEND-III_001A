package com.bancoxyz.batch.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class BatchTaskExecutorConfig {

    @Bean
    public ThreadPoolTaskExecutor batchTaskExecutor() {

        ThreadPoolTaskExecutor executor =
                new ThreadPoolTaskExecutor();

        // Máximo de 4 hilos de ejecución
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(4);

        // Tareas pendientes
        executor.setQueueCapacity(10);

        // Nombre visible en los logs
        executor.setThreadNamePrefix("batch-thread-");

        // Esperar a que terminen las tareas antes de cerrar
        executor.setWaitForTasksToCompleteOnShutdown(true);

        // Tiempo máximo de espera para finalizar
        executor.setAwaitTerminationSeconds(30);

        executor.initialize();

        return executor;
    }
}