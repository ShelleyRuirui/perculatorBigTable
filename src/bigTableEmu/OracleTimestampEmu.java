package bigTableEmu;

public class OracleTimestampEmu {

	private static long timestamp=0;
	
	public synchronized static long getCurTimestamp(){
		timestamp++;
		return timestamp;
	}
}
