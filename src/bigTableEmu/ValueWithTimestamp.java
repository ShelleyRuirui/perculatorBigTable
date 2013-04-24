package bigTableEmu;

public class ValueWithTimestamp {

	String value;
	long timestamp;
	
	public ValueWithTimestamp(String value, long timestamp) {
		this.value = value;
		this.timestamp = timestamp;
	}
	
	public String toString(){
		return timestamp+" "+value;
	}
	
}
