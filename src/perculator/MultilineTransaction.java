package perculator;

import java.util.ArrayList;

import bigTableEmu.BigTable;
import bigTableEmu.Column;
import bigTableEmu.NewRowTransaction;
import bigTableEmu.OracleTimestampEmu;
import bigTableEmu.Row;
import bigTableEmu.TableManager;

public class MultilineTransaction {
	ArrayList<Write> writes=new ArrayList<Write>();
	long start_ts;
	
	public MultilineTransaction(){
		start_ts=OracleTimestampEmu.getCurTimestamp();
	}

	public void set(Row row,Column col,String table,String value){
		Write write=new Write(table,row,col,value);
		writes.add(write);
	}
	
	public String get(Row row,Column col,String tableName){
		BigTable table=TableManager.getTable(tableName);
		NewRowTransaction tr=table.startRowTransaction(row);
		
		//TODO handle finding record in the lock column
		String lock=tr.read(new Column("lock"), 0,start_ts);
		
		String lastWrite=tr.read(new Column("write"), 0,start_ts);
		if(lastWrite==null)
			return null;
		long ts=Long.parseLong(lastWrite);
		String value=tr.read(col, ts,ts);
		return value;
	}
	
	public boolean prewrite(Write w,Write primary){
		Column c=w.col;
		BigTable table=TableManager.getTable(w.table);
		NewRowTransaction tr=table.startRowTransaction(w.row);
		String laterWrite=tr.read(new Column("write"), start_ts);
		if(laterWrite!=null)
			return false;
		
		String beforeLock=tr.read(new Column("lock"), 0,start_ts);
		if(beforeLock!=null)
			return false;
		
		tr.write(c, start_ts, w.value);
		tr.write(new Column("lock"), start_ts, primary.table+"/"+primary.row+"/"+primary.col);
		return tr.commit();
	}
	
	public boolean commit(){
		if(writes.size()==0)
			return true;
		Write primary=writes.get(0);
		if(!prewrite(primary,primary))
			return false;
		for(int i=1;i<writes.size();i++){
			if(!prewrite(writes.get(i),primary))
				return false;
		}
		//TODO if abort no clean??
		long commit_ts=OracleTimestampEmu.getCurTimestamp();
		Write p=primary;
		BigTable table=TableManager.getTable(p.table);
		NewRowTransaction tr=table.startRowTransaction(p.row);
		String primLock=tr.read(new Column("lock"), start_ts,start_ts);
		if(primLock==null)
			return false;
		tr.write(new Column("write"), commit_ts, start_ts+"");
		tr.erase(new Column("lock"),commit_ts);
		if(!tr.commit())
			return false;
		
		for(int i=1;i<writes.size();i++){
			Write w=writes.get(i);
			BigTable curtable=TableManager.getTable(w.table);
			curtable.write(w.row, new Column("write"), commit_ts, start_ts+"");
			curtable.write(w.row, new Column("lock"), commit_ts, null);
		}
		return true;
	}
}
