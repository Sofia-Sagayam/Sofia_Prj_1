package com.stackroute.datamunger.query.parser;

public class Aggregate {
	private String aggreFunc;
	private String column;
	private int result;

	public void setAggreFunc(String aggreFunc) {
		this.aggreFunc = aggreFunc;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public void setResult(int result) {
		this.result = result;
	}

	public String getAggreFunc() {
		return aggreFunc;
	}

	public String getColumn() {
		return column;
	}

	public int getResult() {
		return result;
	}

	public String toString() {
		return getAggreFunc() + "(" + getColumn() + ")" + getResult();
	}

}
