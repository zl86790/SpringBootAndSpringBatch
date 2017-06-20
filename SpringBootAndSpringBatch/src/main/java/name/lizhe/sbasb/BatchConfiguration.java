package name.lizhe.sbasb;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableBatchProcessing
@EnableAutoConfiguration
public class BatchConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private TaskExecutor taskExecutor;

	@Bean
	public TaskExecutor taskExecutor() {
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
		return taskExecutor;
	}

	@Bean
	public Step step1() {
		Tasklet tasklet = new Tasklet() {
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws InterruptedException {
				Thread.sleep(2000);
				System.out.println(Thread.currentThread());
				return RepeatStatus.FINISHED;
			}
		};

		return stepBuilderFactory.get("step1").tasklet(tasklet).taskExecutor(taskExecutor).build();
	}

	@Bean
	public Job job(Step step1) throws Exception {
		return jobBuilderFactory.get("job1").incrementer(new RunIdIncrementer()).start(step1).build();
	}
}