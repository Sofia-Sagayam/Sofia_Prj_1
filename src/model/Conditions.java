package model;

public class Conditions {
	private String colName;
	private String operator;
	private String value;
	
	
	public void setCol_name(String colName) {
		this.colName = colName;
	}
	public void setOperator(String operator) {
		this.operator = operator;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getColName() {
		return colName;
	}
	public String getOperator() {
		return operator;
	}
	public String getValue() {
		return value;
	}
	
	
}
