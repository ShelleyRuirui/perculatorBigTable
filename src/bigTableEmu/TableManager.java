package bigTableEmu;

import java.util.HashMap;
import java.util.Map;

public class TableManager {

	static Map<String,BigTable> tables=new HashMap<String,BigTable>();
	
	public static BigTable getTable(String table){
		return tables.get(table);
	}
	
	public static void setTable(String tableName,BigTable table){
		tables.put(tableName, table);
	}
}
