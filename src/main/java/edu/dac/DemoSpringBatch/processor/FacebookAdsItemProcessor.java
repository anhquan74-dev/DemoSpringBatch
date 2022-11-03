package edu.dac.DemoSpringBatch.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import edu.dac.DemoSpringBatch.model.FacebookAds;

/*
 * 	Để xử lý data sau khi đọc từ file csv, 
 * 	chúng ta sẽ tạo custom processor với class FacebookAdsItemProcessor trong package processor
 */
public class FacebookAdsItemProcessor implements ItemProcessor<FacebookAds, FacebookAds> {

	private static final Logger log = LoggerFactory.getLogger(FacebookAdsItemProcessor.class);

	@Override
	public FacebookAds process(final FacebookAds facebookAds) throws Exception {
		final FacebookAds transformedFacebookAds = new FacebookAds(facebookAds.getDate(), facebookAds.getMedia(),
				facebookAds.getAdnameLPID(), facebookAds.getCost(), facebookAds.getImpression(), facebookAds.getClick(),
				facebookAds.getCv());

		log.info("Converting (" + facebookAds + ") into (" + transformedFacebookAds + ")");

		return transformedFacebookAds;
	}
}
