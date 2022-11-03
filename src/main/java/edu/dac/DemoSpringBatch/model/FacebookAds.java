package edu.dac.DemoSpringBatch.model;

import java.io.Serializable;
import java.sql.Date;

import org.springframework.format.annotation.DateTimeFormat;

public class FacebookAds implements Serializable {

	private String date;
	private String media;
	private String adnameLPID;
	private double cost;
	private int impression;
	private int click;
	private int cv;

	public FacebookAds() {

	};

	public FacebookAds(String date, String media, String adnameLPID, double cost, int impression, int click, int cv) {
		super();
		this.date = date;
		this.media = media;
		this.adnameLPID = adnameLPID;
		this.cost = cost;
		this.impression = impression;
		this.click = click;
		this.cv = cv;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getMedia() {
		return media;
	}

	public void setMedia(String media) {
		this.media = media;
	}

	public String getAdnameLPID() {
		return adnameLPID;
	}

	public void setAdnameLPID(String adnameLPID) {
		this.adnameLPID = adnameLPID;
	}

	public double getCost() {
		return cost;
	}

	public void setCost(double cost) {
		this.cost = cost;
	}

	public int getImpression() {
		return impression;
	}

	public void setImpression(int impression) {
		this.impression = impression;
	}

	public int getClick() {
		return click;
	}

	public void setClick(int click) {
		this.click = click;
	}

	public int getCv() {
		return cv;
	}

	public void setCv(int cv) {
		this.cv = cv;
	}

	@Override
	public String toString() {
		return "FacebookAds [date=" + date + ", media=" + media + ", adnameLPID=" + adnameLPID + ", cost=" + cost
				+ ", impression=" + impression + ", click=" + click + ", cv=" + cv + "]";
	}

}
