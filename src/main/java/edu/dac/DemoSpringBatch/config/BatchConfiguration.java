package edu.dac.DemoSpringBatch.config;

import java.sql.Date;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import edu.dac.DemoSpringBatch.listener.JobCompletionNotificationListener;
import edu.dac.DemoSpringBatch.model.FacebookAds;
import edu.dac.DemoSpringBatch.processor.FacebookAdsItemProcessor;

/*  
 	- Trong package config , ta sẽ tạo class BatchConfiguration 
  	và add 2 annotation @Configuration và @EnableBatchProcessing 
  	để phục vụ cho việc config cho project. 
  	- Cụ thể là @EnableBatchProcessing cung cấp base configuration để building batch job. 
  	Khi đó, các beans sau sẽ được tạo ra để chúng ta có thể autowired bất cứ lúc nào :
		+ JobRepository: bean name "jobRepository"
		+ JobLauncher: bean name "jobLauncher"
		+ JobRegistry: bean name "jobRegistry"
		+ PlatformTransactionManager: bean name "transactionManager"
		+ JobBuilderFactory: bean name "jobBuilders"
		+ StepBuilderFactory: bean name "stepBuilders"
*/
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	/*
	 * JobBuilderFactory và StepBuilderFactory là 2 factories cực kì hữu ích để
	 * build job configuration và jobs steps.
	 */

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	/* ========== Tạo reader, processor và writer cho Job. ============ */

	/* + reader() : Để đọc FacebookCSV_300.csv -> FacebookAds object. */
	@Bean
	public FlatFileItemReader<FacebookAds> reader() {
		return new FlatFileItemReaderBuilder<FacebookAds>().name("FacebookAdsReader")
				.resource(new ClassPathResource("FacebookCSV_3000.csv")).delimited()
				.names(new String[] { "date", "media", "adnameLPID", "cost", "impression", "click", "cv" })
				.linesToSkip(1).fieldSetMapper(new BeanWrapperFieldSetMapper<FacebookAds>() {
					{
						setTargetType(FacebookAds.class);
					}
				}).build();
	}

	/* + processor(): định nghĩa Bean method của processor */
	@Bean
	public FacebookAdsItemProcessor processor() {
		return new FacebookAdsItemProcessor();
	}

	/*
	 * + writer(DataSource dataSource) : Write mỗi FacebookAds item vào trong
	 * database, trong đó dataSource đã được khởi tạo tự động bằng
	 * anotation @EnableBatchProcessing theo những config trong file pom và
	 * schema.sql . báo warning skip ko lấy sau ni sửa xong file chạy lại thì nó sẽ
	 * ghi tiếp những Ad name - LPID + date chưa có trong database. Những cái có rồi
	 * ko ghi lại
	 */
	@Bean
	public JdbcBatchItemWriter<FacebookAds> writer() {
		JdbcBatchItemWriter<FacebookAds> writer = new JdbcBatchItemWriter<FacebookAds>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<FacebookAds>());
		writer.setSql("INSERT INTO facebook_ads (Date, Media, AdnameLPID, Cost, Impression, Click, CV)"
				+ " values (:date, :media, :adnameLPID, :cost, :impression, :click, :cv)");
		writer.setDataSource(dataSource);
		return writer;
	}

	/* =========== Cấu hình Step và Job =========== */
	@Bean
	public Job importFacebookAdsJob(JobCompletionNotificationListener listener) {
		return jobBuilderFactory.get("importFacebookAdsJob").incrementer(new RunIdIncrementer()).listener(listener)
				.flow(step1()).end().build();
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<FacebookAds, FacebookAds>chunk(10).reader(reader())
				.processor(processor()).writer(writer()).build();
	}

}
