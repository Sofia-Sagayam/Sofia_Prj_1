package com.stackroute.datamunger.query.parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParser implements Cloneable {

	private String typeOfQuery;
	private QueryParameter queryParameter = new QueryParameter();

	public QueryParameter getQueryParameter() {
		return queryParameter;
	}

	public void setQueryParameter(QueryParameter queryParameter) {
		this.queryParameter = queryParameter;
	}

	public String getTypeOfQuery() {
		return typeOfQuery;
	}

	public void setTypeOfQuery(String typeOfQuery) {
		this.typeOfQuery = typeOfQuery;
	}

	private List<Aggregate> getAggregates(List<String> fields) {
		typeOfQuery = "AGGREGATE";
		int length = queryParameter.getFeilds().size();
		Aggregate aggregateValue[] = new Aggregate[length];
		int fieldIndex = 0;
		int aggregateIndex = 0;
		for (String s : fields) {
			Pattern pattern = Pattern.compile("\\((.*?)\\)");
			Matcher matcher = pattern.matcher(s);
			if (matcher.find()) {

				aggregateValue[aggregateIndex] = new Aggregate();
				aggregateValue[aggregateIndex].setColumn(fields.get(fieldIndex).split("\\(")[1].trim().split("\\)")[0].trim());
				aggregateValue[aggregateIndex].setAggreFunc(fields.get(fieldIndex).split("\\(")[0].trim());
				aggregateIndex++;
			}
			fieldIndex++;
		}

		return Arrays.asList(aggregateValue);
	}

	private String getGroupBy(String queryString) {
		typeOfQuery = "GROUP_BY";

		return queryString.split("group by")[1].trim().split("order by")[0].trim().split("\\s+")[0];
	}

	private String getOrderBy(String queryString) {
		return queryString.split("order by")[1].trim().split("\\s+")[0];
	}

	private List getWhereCondition(String queryString) {
		List<String> whereCondition = Arrays.asList(queryString.split("where")[1].trim().split("order by")[0].trim().split("group by")[0].trim().split("AND|OR"));
		int length = whereCondition.size();
		String logicalOperator[] = new String[length - 1];
		for (int i = 0; i < length - 1; i++) {
			logicalOperator[i] = new String();
			logicalOperator[i] = queryString.split(whereCondition.get(i))[1].trim().split(whereCondition.get(i + 1))[0].trim();
		}
		queryParameter.setLogicalOperators(Arrays.asList(logicalOperator));
		Conditions conditions[] = new Conditions[length];
		for (int i = 0; i < length; i++) {
			conditions[i] = new Conditions();
			conditions[i].setCol_name(whereCondition.get(i).split("<=|>=|!=|<|>|=")[0].trim());
			conditions[i].setValue(whereCondition.get(i).split("<=|>=|!=|<|>|=")[1].trim());
			conditions[i].setOperator(
					whereCondition.get(i).split(conditions[i].getColName())[1].split(conditions[i].getValue())[0]
							.trim());

		}
		return Arrays.asList(conditions);
	}

	private String getPath(String queryString) {
		typeOfQuery = "SIMPLE";
		return queryString.split("from")[1].trim().split("\\s+")[0];
	}

	public QueryParser parseQuery(String queryString) {

		if (queryString.contains("group by")) {
			queryParameter.setGroupByFeild(getGroupBy(queryString));
		}
		if (queryString.contains("order by")) {
			queryParameter.setOrderByFeild(getOrderBy(queryString));
		}
		if (queryString.contains("where")) {

			queryParameter.setConditions(getWhereCondition(queryString));

		}
		if (queryString.contains("from")) {

			queryParameter.setPath(getPath(queryString));
		}
		if (queryString.contains("select")) {
			String columns = queryString.split("from")[0].trim().split("select")[1];

			if (!columns.equals("*")) {
				String splitedColumnName[] = columns.trim().split(",");
				for (int c = 0; c < splitedColumnName.length; c++) {
					queryParameter.getFeilds().add(splitedColumnName[c]);
				}
				if (columns.contains("sum") || columns.contains("avg") || columns.contains("min")
						|| columns.contains("max") || columns.contains("count")) {
					queryParameter.setAggregates(getAggregates(queryParameter.getFeilds()));
				}
			} else {
				queryParameter.getFeilds().add("*");
			}
		}
		if (queryParameter.getGroupByFeild() != null) {
			typeOfQuery = "GROUP_BY";
		}
		if (queryParameter.getGroupByFeild() != null && !queryParameter.getAggregates().isEmpty()) {
			typeOfQuery = "GROUP_BY_AGGREGATE";
		}

		return this;
	}

}
