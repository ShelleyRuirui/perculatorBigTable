package com.bigTableEmu;

public class ValueWithTimestamp {

	String value;
	long timestamp;
	
	public ValueWithTimestamp(String value, long timestamp) {
		this.value = value;
		this.timestamp = timestamp;
	}
	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String toString(){
		return timestamp+" "+value;
	}
	
}
