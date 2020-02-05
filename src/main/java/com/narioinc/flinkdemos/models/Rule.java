package com.narioinc.flinkdemos.models;

public class Rule {
	private int mOccurence;
	private long mDuration;
	
	public Rule(int occurence, long durationMillis){
		mOccurence = occurence;
		mDuration = durationMillis;
	}
	
	public int getOccurence() {
		return mOccurence;
	}
	public void setOccurence(int occurence) {
		this.mOccurence = occurence;
	}
	public long getDuration() {
		return mDuration;
	}
	public void setDuration(long duration) {
		this.mDuration = duration;
	}
}
