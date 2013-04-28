package perculator;

import java.util.ArrayList;

import bigTableEmu.BigTable;
import bigTableEmu.Column;
import bigTableEmu.NewRowTransaction;
import bigTableEmu.OracleTimestampEmu;
import bigTableEmu.Row;
import bigTableEmu.TableManager;
import bigTableEmu.ValueWithTimestamp;

public class MultilineTransaction {
	ArrayList<Write> writes=new ArrayList<Write>();
	long start_ts;
	long commit_ts;
	NewRowTransaction buffertr; //For single step demo
	
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
		
		buffertr=tr;
		checkIfNeedClean(row,col,tableName);
		
		String lastWrite=tr.read(new Column("write"), 0,start_ts);
		if(lastWrite==null)
			return null;
		long ts=Long.parseLong(lastWrite);
		String value=tr.read(col, ts,ts);
		return value;
	}
	
	public void beforeClean(Row row,Column col,String tableName){
		BigTable table=TableManager.getTable(tableName);
		NewRowTransaction tr=table.startRowTransaction(row);		
		buffertr=tr;
	}
	
	public String afterClean(Row row,Column col,String tableName){
		NewRowTransaction tr=buffertr;
		String lastWrite=tr.read(new Column("write"), 0,start_ts);
		if(lastWrite==null)
			return null;
		long ts=Long.parseLong(lastWrite);
		String value=tr.read(col, ts,ts);
		return value;
	}
	
	public void checkIfNeedClean(Row row,Column col,String tableName){
		BigTable table=TableManager.getTable(tableName);
		NewRowTransaction tr=buffertr;
		ValueWithTimestamp lock=tr.read(new Column("lock"), 0,start_ts,1);
		//If it exist with a non null value
		if(lock!=null&&lock.getValue()!=null){
			System.out.println(lock);
			String[] infos=lock.getValue().split("-");
			String lockTable=infos[0];
			String lockRow=infos[1];
			
			//If it's not the primary lock
			if(!lockTable.equals(tableName) || !lockRow.equals(row.toString())){
				BigTable primaryTable=table;
				if(!lockTable.equals(tableName))
					primaryTable=TableManager.getTable(lockTable);
				boolean result=primaryTable.findByValue(new Row(lockRow), new Column("write"), lock.getTimestamp()+"");
				if(result){
					//continue process commit and OK now
					NewRowTransaction trcommit=table.startRowTransaction(row);
//					long current_ts=OracleTimestampEmu.getCurTimestamp();
					trcommit.write(new Column("write"), start_ts, lock.getTimestamp()+"");
					trcommit.erase(new Column("lock"), lock.getTimestamp());
					trcommit.commit();
					return;
				}else{
					//It the secondary but primary didn't commit
					NewRowTransaction trclean=primaryTable.startRowTransaction(new Row(lockRow));
					trclean.erase(new Column("lock"), lock.getTimestamp());
					trclean.commit();
					
					NewRowTransaction trclean2=table.startRowTransaction(row);
					trclean2.erase(new Column("lock"), lock.getTimestamp());
					trclean2.commit();
				}
			}
			
			//If it's primary and not committed
			//Check chubby to see if it will clean up
			NewRowTransaction trclean=table.startRowTransaction(row);
			trclean.erase(new Column("lock"), lock.getTimestamp());
			trclean.commit();
		}
	}
	
	
	
	public boolean prewrite(Write w,Write primary){
		Column c=w.col;
		BigTable table=TableManager.getTable(w.table);
		NewRowTransaction tr=table.startRowTransaction(w.row);
		String laterWrite=tr.read(new Column("write"), start_ts);
		if(laterWrite!=null)
			return false;
		
		String beforeLock=tr.read(new Column("lock"), 0);
		if(beforeLock!=null)
			return false;
		
		tr.write(c, start_ts, w.value);
		tr.write(new Column("lock"), start_ts, primary.table+"-"+primary.row);
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
		commit_ts=OracleTimestampEmu.getCurTimestamp();
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
	
	public boolean checkHaveWrites(){
		if(writes.size()==0)
			return false;
		return true;
	}
	
	public boolean preWritePrimary(){
		Write w=writes.get(0);
		if(!prewrite(w,w))
			return false;
		return true;
	}
	
	public boolean prewriteSecondary(){
		Write primary=writes.get(0);
		for(int i=1;i<writes.size();i++){
			if(!prewrite(writes.get(i),primary))
				return false;
		}
		return true;
	}
	
	public boolean commitPrimary(){
		Write primary=writes.get(0);
		commit_ts=OracleTimestampEmu.getCurTimestamp();
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
		return true;
	}
	
	public void commitSecondary(){
		for(int i=1;i<writes.size();i++){
			Write w=writes.get(i);
			BigTable curtable=TableManager.getTable(w.table);
			curtable.write(w.row, new Column("write"), commit_ts, start_ts+"");
			curtable.write(w.row, new Column("lock"), commit_ts, null);
		}
	}
}
