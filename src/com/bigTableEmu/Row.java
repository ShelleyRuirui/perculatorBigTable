package com.bigTableEmu;

public class Row {

	private String rowKey;

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public Row(String rowKey) {
		this.rowKey = rowKey;
	}
	
	public boolean equals(Object o){
		if(o instanceof Row)
			return ((Row) o).rowKey.equals(this.rowKey);
		return false;
	}
	
	public String toString(){
		return rowKey;
	}
	
	public int hashCode(){
		return toString().hashCode();
	}
	
	public static void main(String[] args){
		Row r1=new Row("r1");
		Row r2=new Row("r1");
		System.out.println(r1.equals(r2));
	}
}
