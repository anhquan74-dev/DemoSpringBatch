package edu.dac.DemoSpringBatch.listener;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import edu.dac.DemoSpringBatch.model.FacebookAds;

/*
 * 	Với mục đích để check xem Job đã tạo ra hoạt động tốt hay chưa,
 * 	ta sẽ tạo class JobCompletionNotificationListener
 * 	được extend từ class JobExecutionListenerSupport 
 *  để select thông tin từ table information
 */

@Component /*
			 * @Component là một Annotation (chú thích) đánh dấu trên các Class để giúp
			 * Spring biết nó là một Bean.
			 */
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	/*
	 * - Hàm afterJob sẽ tự động được thực thi ngay sau khi Job của chúng ta kết thúc. 
	 * 		+ JobExecution là "primary storage mechanism" thể hiện cho chúng ta
	 * 		thấy tất cả những gì sẽ diễn ra suốt quá trình chạy Job. 
	 * 		+ Status chính là một property của trong JobExcution, 
	 * 		được thể hiện qua BatchStatus - là một enumeration gồm các values sau : 
	 * 			[ COMPLETED, STARTING, STARTED, STOPPING, STOPPED, FAILED, ABANDONED, UNKNOWN ] 
	 * 			Trong trường hợp của project : Nếu status là 'COMPLETED' thì 
	 * 			ta sẽ thực hiện query để lấy ra thông tin trong table information.
	 */
	@Override
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("!!! JOB FINISHED! Time to verify the results");

			List<FacebookAds> results = jdbcTemplate.query(
					"SELECT Date, Media, AdnameLPID, Cost, Impression, Click, CV FROM facebook_ads",
					new RowMapper<FacebookAds>() {
						@Override
						public FacebookAds mapRow(ResultSet rs, int row) throws SQLException {
							return new FacebookAds(rs.getString(1), rs.getString(2),rs.getString(3),rs.getDouble(4),rs.getInt(5),rs.getInt(6),rs.getInt(7));
						}
					});

			for (FacebookAds facebookAds : results) {
				log.info("Found <" + facebookAds + "> in the database.");
			}

		}
	}

}
