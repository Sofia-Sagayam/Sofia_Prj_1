package com.stackroute.datamunger.query.parser;

import java.util.ArrayList;
import java.util.List;

public class QueryParameter implements Cloneable {
	private String groupByFeild;
	private String orderByFeild;
	private String path;
	private List<Conditions> conditions = new ArrayList<>();
	private List<String> logicalOperators = new ArrayList<>();
	private List<String> feilds = new ArrayList<>();
	private List<Aggregate> aggregates = new ArrayList<>();

	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	public String getGroupByFeild() {
		return groupByFeild;
	}

	public void setGroupByFeild(String groupByFeild) {
		this.groupByFeild = groupByFeild;
	}

	public String getOrderByFeild() {
		return orderByFeild;
	}

	public void setOrderByFeild(String orderByFeild) {
		this.orderByFeild = orderByFeild;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public List<Conditions> getConditions() {
		return conditions;
	}

	public void setConditions(List<Conditions> conditions) {
		this.conditions = conditions;
	}

	public List<String> getLogicalOperators() {
		return logicalOperators;
	}

	public void setLogicalOperators(List<String> logicalOperators) {
		this.logicalOperators = logicalOperators;
	}

	public List<String> getFeilds() {
		return feilds;
	}

	public void setFeilds(List<String> feilds) {
		this.feilds = feilds;
	}

	public List<Aggregate> getAggregates() {
		return aggregates;
	}

	public void setAggregates(List<Aggregate> aggregates) {
		this.aggregates = aggregates;
	}

}
