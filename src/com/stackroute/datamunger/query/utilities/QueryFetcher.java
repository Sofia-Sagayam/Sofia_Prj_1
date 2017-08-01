package com.stackroute.datamunger.query.utilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.query.parser.Row;

public class QueryFetcher {
	private Map<String, Integer> header;
Header headerObject=new Header();
	public boolean restrictionCheck(String aftersplit[], QueryParser parsedQuery) {
		header = headerObject.headerMap(parsedQuery.getQueryParameter().getPath());
		int logicalOperators = parsedQuery.getQueryParameter().getLogicalOperators().size() + 1;
		boolean criteria[] = new boolean[logicalOperators];
		for (int logOp = 0; logOp < logicalOperators; logOp++) {
			if (parsedQuery.getQueryParameter().getConditions().get(logOp).getOperator().equals("=")) {
				criteria[logOp] = aftersplit[header
						.get(parsedQuery.getQueryParameter().getConditions().get(logOp).getColName())]
								.equals(parsedQuery.getQueryParameter().getConditions().get(logOp).getValue());
			} else if (parsedQuery.getQueryParameter().getConditions().get(logOp).getOperator().equals("!=")) {
				criteria[logOp] = !(aftersplit[header
						.get(parsedQuery.getQueryParameter().getConditions().get(logOp).getColName())]
								.equals(parsedQuery.getQueryParameter().getConditions().get(logOp).getValue()));
			} else if (parsedQuery.getQueryParameter().getConditions().get(logOp).getOperator().equals(">")) {
				long res = Math.max(
						Long.parseLong(aftersplit[header
								.get(parsedQuery.getQueryParameter().getConditions().get(logOp).getColName())]),
						Long.parseLong(parsedQuery.getQueryParameter().getConditions().get(logOp).getValue()));
				criteria[logOp] = Long.toString(res)
						.equals(aftersplit[header
								.get(parsedQuery.getQueryParameter().getConditions().get(logOp).getColName())])
						&& !Long.toString(res)
								.equals(parsedQuery.getQueryParameter().getConditions().get(logOp).getValue());
			} else if (parsedQuery.getQueryParameter().getConditions().get(logOp).getOperator().equals("<")) {
				long res = Math.min(
						Long.parseLong(aftersplit[header
								.get(parsedQuery.getQueryParameter().getConditions().get(logOp).getColName())]),
						Long.parseLong(parsedQuery.getQueryParameter().getConditions().get(logOp).getValue()));
				criteria[logOp] = Long.toString(res)
						.equals(aftersplit[header
								.get(parsedQuery.getQueryParameter().getConditions().get(logOp).getColName())])
						&& !Long.toString(res)
								.equals(parsedQuery.getQueryParameter().getConditions().get(logOp).getValue());

			} else if (parsedQuery.getQueryParameter().getConditions().get(logOp).getOperator().equals(">=")) {
				long res = Math.max(
						Long.parseLong(aftersplit[header
								.get(parsedQuery.getQueryParameter().getConditions().get(logOp).getColName())]),
						Long.parseLong(parsedQuery.getQueryParameter().getConditions().get(logOp).getValue()));
				criteria[logOp] = Long.toString(res).equals(aftersplit[header
						.get(parsedQuery.getQueryParameter().getConditions().get(logOp).getColName())]);
			} else if (parsedQuery.getQueryParameter().getConditions().get(logOp).getOperator().equals("<=")) {
				long res = Math.min(
						Long.parseLong(aftersplit[header
								.get(parsedQuery.getQueryParameter().getConditions().get(logOp).getColName())]),
						Long.parseLong(parsedQuery.getQueryParameter().getConditions().get(logOp).getValue()));
				criteria[logOp] = Long.toString(res).equals(aftersplit[header
						.get(parsedQuery.getQueryParameter().getConditions().get(logOp).getColName())]);

			}
		}
		boolean finalResult = criteria[0];
		for (int god = 0; god < logicalOperators - 1; god++) {

			if (parsedQuery.getQueryParameter().getLogicalOperators().get(god).equals("AND")) {
				finalResult = finalResult && criteria[god + 1];
			} else if (parsedQuery.getQueryParameter().getLogicalOperators().get(god).equals("OR")) {
				finalResult = finalResult || criteria[god + 1];
			}
		}
		return finalResult;
	}

	public Map<Long, Row> getData(QueryParser parsedQuery) {
		header = headerObject.headerMap(parsedQuery.getQueryParameter().getPath());
		Map<Long, Row> dataSet = new HashMap<>();
		BufferedReader reader = null;
		String tuple = "";
		if (parsedQuery.getQueryParameter().getFeilds().get(0).equals("*")) {
			Row<String, String> rows;
			Long index = new Long(1);
			if (!parsedQuery.getQueryParameter().getConditions().isEmpty()) {
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getQueryParameter().getPath()));
					String headerValues[] = reader.readLine().split(",");
					tuple = reader.readLine();
					int i = 0;
					while (tuple != null) {
						rows = new Row<>();
						String row[] = tuple.split(",");
						int len = row.length;

						if (restrictionCheck(row, parsedQuery)) {
							for (i = 0; i < len; i++) {
								rows.put(headerValues[header.get((headerValues[i]))], row[header.get(headerValues[i])]);
							}
							dataSet.put(index, rows);
							index++;
						}
						tuple = reader.readLine();

					}
				} catch (IOException io) {

				}
				return dataSet;
			} else {
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getQueryParameter().getPath()));
					String headerValues[] = reader.readLine().split(",");
					tuple = reader.readLine();
					int i = 0;
					while (tuple != null) {
						rows = new Row<>();
						String row[] = tuple.split(",");
						int len = row.length;
						for (i = 0; i < len; i++) {
							rows.put(headerValues[header.get((headerValues[i]))], row[header.get(headerValues[i])]);
						}
						dataSet.put(index, rows);
						tuple = reader.readLine();
						index++;
					}
				} catch (IOException io) {

				}
				return dataSet;
			}

		} else {
			Row<String, String> rows;
			Long index = new Long(1);
			if (!parsedQuery.getQueryParameter().getConditions().isEmpty()) {
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getQueryParameter().getPath()));
					String headers[] = reader.readLine().split(",");
					tuple = reader.readLine();
					int i = 0;
					while (tuple != null) {
						rows = new Row<>();
						String row[] = tuple.split(",");
						int length = parsedQuery.getQueryParameter().getFeilds().size();
						if (parsedQuery.getQueryParameter().getOrderByFeild() != null) {
							parsedQuery.getQueryParameter().getFeilds()
									.add(parsedQuery.getQueryParameter().getOrderByFeild());
							length += 1;
						}
						if (restrictionCheck(row, parsedQuery)) {
							for (i = 0; i < length; i++) {
								rows.put(headers[header.get(parsedQuery.getQueryParameter().getFeilds().get(i))],
										row[header.get(parsedQuery.getQueryParameter().getFeilds().get(i))]);
							}
							dataSet.put(index, rows);
							index++;
						}
						tuple = reader.readLine();

					}
				} catch (IOException io) {

				}
				return dataSet;
			} else {
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getQueryParameter().getPath()));
					String headerValues[] = reader.readLine().split(",");
					tuple = reader.readLine();
					int i = 0;
					while (tuple != null) {
						rows = new Row<>();
						String row[] = tuple.split(",");
						int length = parsedQuery.getQueryParameter().getFeilds().size();
						if (parsedQuery.getQueryParameter().getOrderByFeild() != null) {
							parsedQuery.getQueryParameter().getFeilds()
									.add(parsedQuery.getQueryParameter().getOrderByFeild());
							length += 1;
						}
						for (i = 0; i < length; i++) {
							rows.put(headerValues[header.get(parsedQuery.getQueryParameter().getFeilds().get(i))],
									row[header.get(parsedQuery.getQueryParameter().getFeilds().get(i))]);
						}
						dataSet.put(index, rows);
						tuple = reader.readLine();
						index++;
					}
				} catch (IOException io) {

				}
				return dataSet;
			}
		}

	}

	}