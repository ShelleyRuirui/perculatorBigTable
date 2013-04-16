package bigTableEmu;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BigTable table=new BigTable();
		
		ColumnData cd1=new ColumnData();
		cd1.addValue(1, "test1_1_1");
		cd1.addValue(2, "test1_1_2");
		cd1.addValue(7, "test1_1_3");
		
		RowData rd1=new RowData();
		
		Column col1=new Column("col_1");
		rd1.addColumnAndData(col1, cd1);
		
		Row row1=new Row("row1");
		table.addRowAndData(row1, rd1);
		
		RowTransaction tr=table.startRowTransaction(row1);
		String result=tr.read(new Row("row1"), new Column("col_1"), 1,5);
		System.out.println(result);
		
		result=tr.read(new Row("row1"), new Column("col_1"), 8,9);
		System.out.println(result);
		
		tr.write(row1, col1, 10, "Hello10");
		result=tr.read(new Row("row1"), new Column("col_1"), 8,10);
		System.out.println(result);
		
		tr.write(row1, col1, 5, "Hello5");
		result=tr.read(new Row("row1"), new Column("col_1"), 2,6);
		System.out.println(result);
	}

}
