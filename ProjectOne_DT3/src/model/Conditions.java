package model;

public class Conditions {
	private String col_name;
	private String operator;
	private String value;
	
	
	public void setCol_name(String col_name) {
		this.col_name = col_name;
	}
	public void setOperator(String operator) {
		this.operator = operator;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getCol_name() {
		return col_name;
	}
	public String getOperator() {
		return operator;
	}
	public String getValue() {
		return value;
	}
	
	
}
