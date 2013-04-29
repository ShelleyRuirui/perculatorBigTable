package com.perculator;

import com.bigTableEmu.BigTable;
import com.bigTableEmu.Column;
import com.bigTableEmu.Row;

public class Write {

	Row row;
	Column col;
	String value;
	String table;
	
	public Write(String table,Row row, Column col, String value) {
		this.table=table;
		this.row = row;
		this.col = col;
		this.value = value;
	}
	
	public String toString(){
		return "Table:"+table+"Row:"+row+"Column:"+col+"Value:"+value;
	}
	
}
