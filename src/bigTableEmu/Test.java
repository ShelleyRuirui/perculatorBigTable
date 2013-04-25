package bigTableEmu;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		test4();
	}
	
	public static void test1(){
		BigTable table=new BigTable();
		
		ColumnData cd1=new ColumnData();
		cd1.write(1, "test1_1_1");
		cd1.write(2, "test1_1_2");
		cd1.write(7, "test1_1_3");
		
		RowData rd1=new RowData();
		
		Column col1=new Column("col_1");
		rd1.addColumnAndData(col1, cd1);
		
		Row row1=new Row("row1");
		table.addRowAndData(row1, rd1);
		
		NewRowTransaction tr=table.startRowTransaction(row1);
		String result=tr.read(new Column("col_1"), 1,5);
		System.out.println(result);
		
		result=tr.read(new Column("col_1"), 7,7);
		System.out.println(result);
		
		tr.write(col1, 10, "Hello10");
		result=tr.read(new Column("col_1"), 11,12);
		System.out.println(result);
		
		tr.write( col1, 5, "Hello5");
		result=tr.read( new Column("col_1"), 2,6);
		System.out.println(result);
		
		tr.erase(col1, 6);
		result=tr.read( new Column("col_1"), 2,6);
		System.out.println(result);
		
		NewRowTransaction tr2=table.startRowTransaction(row1);
		result=tr2.read( new Column("col_1"), 9,12);
		System.out.println(result);
		
		tr.commit();
		result=tr2.read( new Column("col_1"), 9,12);
		System.out.println(result);
	}
	
	public static void test2(){
		BigTable table=new BigTable();
		long start_ts=OracleTimestampEmu.getCurTimestamp();
		long start_ts2=OracleTimestampEmu.getCurTimestamp();
		NewRowTransaction tr1=table.startRowTransaction(new Row("row"));
		NewRowTransaction tr2=table.startRowTransaction(new Row("row"));
		tr1.write(new Column("col"), start_ts, "TR1");
		tr2.write(new Column("col"), start_ts2, "TR2");
		System.out.println(tr1.commit());
		System.out.println(tr2.commit());
		table.print();
	}
	
	public static void test3(){
		BigTable table=new BigTable();
		long start_ts=OracleTimestampEmu.getCurTimestamp();
		long start_ts2=OracleTimestampEmu.getCurTimestamp();
		NewRowTransaction tr1=table.startRowTransaction(new Row("row"));
		NewRowTransaction tr2=table.startRowTransaction(new Row("row"));
		tr1.write(new Column("col"), start_ts, "TR1");
		System.out.println(tr1.commit());
		tr2.write(new Column("col"), start_ts2, "TR2");		
		System.out.println(tr2.commit());
		table.print();
	}
	
	//NewRowTransaction is actually non repeatable here, but it's desired. Demonstrated here.
	public static void test4(){
		BigTable table=new BigTable();
		long start_ts=OracleTimestampEmu.getCurTimestamp();
		long start_ts2=OracleTimestampEmu.getCurTimestamp();
		NewRowTransaction tr1=table.startRowTransaction(new Row("row"));
		NewRowTransaction tr2=table.startRowTransaction(new Row("row"));
		
		System.out.println(tr2.read(new Column("col"), 0));
		tr1.write(new Column("col"), start_ts, "TR1");
		System.out.println(tr1.commit());
		System.out.println(tr2.read(new Column("col"), 0));
		tr2.write(new Column("col"), start_ts2, "TR2");		
		System.out.println(tr2.commit());
		table.print();
	}

}
