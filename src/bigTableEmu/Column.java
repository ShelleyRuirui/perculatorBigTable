package bigTableEmu;

public class Column {

	private String colName;

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public Column(String colName) {
		this.colName = colName;
	}
	
	public boolean equals(Object o){
		if(o instanceof Column)
			return ((Column) o).colName.equals(this.colName);
		return false;
	}
	
	public String toString(){
		return colName;
	}
	
	public int hashCode(){
		return toString().hashCode();
	}
}
