package bigTableEmu;

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
}
