package com.bancoxyz.batch.listener;

import java.time.LocalDateTime;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class BatchJobListener implements JobExecutionListener {

    private final ApplicationContext applicationContext;

    public BatchJobListener(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {

        System.out.println();
        System.out.println("==================================================");
        System.out.println("INICIO DE JOB");
        System.out.println("==================================================");

        System.out.println(
                "Job: "
                        + jobExecution.getJobInstance().getJobName()
        );

        System.out.println(
                "Execution ID: "
                        + jobExecution.getId()
        );

        System.out.println(
                "Inicio: "
                        + LocalDateTime.now()
        );

        System.out.println("==================================================");
        System.out.println();
    }

    @Override
    public void afterJob(JobExecution jobExecution) {

        System.out.println();
        System.out.println("==================================================");
        System.out.println("FIN DE JOB");
        System.out.println("==================================================");

        System.out.println(
                "Job: "
                        + jobExecution.getJobInstance().getJobName()
        );

        System.out.println(
                "Execution ID: "
                        + jobExecution.getId()
        );

        System.out.println(
                "Estado: "
                        + jobExecution.getStatus()
        );

        System.out.println(
                "Exit Status: "
                        + jobExecution.getExitStatus().getExitCode()
        );

        System.out.println(
                "Inicio: "
                        + jobExecution.getStartTime()
        );

        System.out.println(
                "Fin: "
                        + jobExecution.getEndTime()
        );

        if (jobExecution.getStatus().isUnsuccessful()) {

            System.out.println();
            System.out.println("El Job finalizó con errores.");

            jobExecution.getAllFailureExceptions()
                    .forEach(exception -> {

                        System.out.println(
                                "Error: "
                                        + exception.getMessage()
                        );
                    });

        } else {

            System.out.println();
            System.out.println("El Job finalizó correctamente.");
        }

        System.out.println("==================================================");
        System.out.println();


    }
}
