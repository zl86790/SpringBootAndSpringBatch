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
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.validation.BindException;

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
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(10);
		return taskExecutor;
	}

	@Bean
	public Step step1() {
		Tasklet tasklet = new Tasklet() {
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws InterruptedException {
				System.out.println(Thread.currentThread());
				return RepeatStatus.FINISHED;
			}
		};
		
		FlatFileItemReader<String> reader = new FlatFileItemReader<String>();
		DefaultLineMapper<String> lineMapper = new DefaultLineMapper<String>();
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setDelimiter(",");
		FieldSetMapper fsMapper = new FieldSetMapper<Object>(){
			@Override
			public String mapFieldSet(FieldSet fieldset) throws BindException {
				return fieldset.readRawString(0)+fieldset.readRawString(1)+Thread.currentThread();
			}
		};
		lineMapper.setLineTokenizer(tokenizer);
		lineMapper.setFieldSetMapper(fsMapper);
		
		reader.setResource(new FileSystemResource("c://test.csv"));
		reader.setLineMapper(lineMapper);
		ItemProcessor processor = new ConsoleItemProcessor();
		

		return stepBuilderFactory.get("step1").chunk(2).reader(reader).processor(processor).taskExecutor(taskExecutor).throttleLimit(1).build();
	}

	@Bean
	public Job job(Step step1) throws Exception {
		return jobBuilderFactory.get("job1").incrementer(new RunIdIncrementer()).start(step1).build();
	}
}