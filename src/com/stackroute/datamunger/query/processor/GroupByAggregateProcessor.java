package com.stackroute.datamunger.query.processor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.query.parser.Row;
import com.stackroute.datamunger.query.utilities.Header;
import com.stackroute.datamunger.query.utilities.QueryFetcher;

public class GroupByAggregateProcessor implements QueryProcessor {
	
	@Override
	public Map executeQuery(QueryParser parsedQuery) {
		QueryFetcher data = new QueryFetcher();
		Header headerObject=new Header();
		Map<String, Integer> header = headerObject.headerMap(parsedQuery.getQueryParameter().getPath());
		Row<String, Integer> rowSet = new Row<>();
		Map<Long, Row> dataSet = new HashMap<>();
		List<String> fields = parsedQuery.getQueryParameter().getFeilds();
		String groupByColumn = parsedQuery.getQueryParameter().getGroupByFeild();
		if (fields.contains(groupByColumn)) {
			BufferedReader reader = null;
			String tuple = "";
			
			Long index = new Long(1);
			String aggregateFunc = parsedQuery.getQueryParameter().getAggregates().get(0).getAggreFunc();
			String aggregateColumn = parsedQuery.getQueryParameter().getAggregates().get(0).getColumn();
			if (parsedQuery.getQueryParameter().getConditions().isEmpty()) {
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getQueryParameter().getPath()));
					String headerValues[] = reader.readLine().split(",");
					tuple = reader.readLine();
					int sum = 0;
					String row[];
					switch (aggregateFunc) {
					case "sum":

						while (tuple != null) {
							row = tuple.split(",");

							if (rowSet.get(row[header.get(groupByColumn)]) != null) {
								sum = Integer.parseInt(rowSet.get(row[header.get(groupByColumn)]).toString());
								sum += Integer.parseInt(row[header.get(aggregateColumn)]);
								rowSet.put(row[header.get(groupByColumn)], sum);
								sum = 0;
							} else {
								rowSet.put(row[header.get(groupByColumn)],Integer.parseInt(row[header.get(aggregateColumn)]));

							}
							tuple = reader.readLine();

						}
						break;
					case "avg":

						int avgCount = 0;
						while (tuple != null) {
							row = tuple.split(",");

							if (rowSet.get(row[header.get(groupByColumn)]) != null) {
								sum = Integer.parseInt(rowSet.get(row[header.get(groupByColumn)]).toString());
								avgCount++;
								sum += Integer.parseInt(row[header.get(aggregateColumn)]);
								rowSet.put(row[header.get(groupByColumn)], sum / avgCount);
								sum = 0;
							} else {
								rowSet.put(row[header.get(groupByColumn)],
										Integer.parseInt(row[header.get(aggregateColumn)]));

							}
							tuple = reader.readLine();

						}
						break;
					case "min":
						int minInteger = 0;
						while (tuple != null) {
							 row = tuple.split(",");

							if (rowSet.get(row[header.get(groupByColumn)]) != null) {
								minInteger = Integer.parseInt(rowSet.get(row[header.get(groupByColumn)]).toString());

								if (Integer.parseInt(row[header.get(aggregateColumn)]) < minInteger)
									minInteger = Integer.parseInt(row[header.get(aggregateColumn)]);
								rowSet.put(row[header.get(groupByColumn)], minInteger);

							} else {
								rowSet.put(row[header.get(groupByColumn)],
										Integer.parseInt(row[header.get(aggregateColumn)]));

							}
							tuple = reader.readLine();

						}
						break;
					case "max":
						int maxInteger = 0;
						while (tuple != null) {
							row= tuple.split(",");

							if (rowSet.get(row[header.get(groupByColumn)]) != null) {
								maxInteger = Integer.parseInt(rowSet.get(row[header.get(groupByColumn)]).toString());

								if (Integer.parseInt(row[header.get(aggregateColumn)]) > maxInteger)
									minInteger = Integer.parseInt(row[header.get(aggregateColumn)]);
								rowSet.put(row[header.get(groupByColumn)], maxInteger);

							} else {
								rowSet.put(row[header.get(groupByColumn)],
										Integer.parseInt(row[header.get(aggregateColumn)]));

							}
							tuple = reader.readLine();

						}
						break;
					case "count":
						int count = 0;
						while (tuple != null) {
							String r[] = tuple.split(",");

							if (rowSet.get(r[header.get(groupByColumn)]) != null) {
								count++;

								rowSet.put(r[header.get(groupByColumn)], count);

							} else {

								rowSet.put(r[header.get(groupByColumn)], 1);

							}
							tuple = reader.readLine();

						}
						break;

					}

				}

				catch (IOException io) {

				}
				dataSet.put(index, rowSet);

				
			}
		}
		return dataSet;
	}

}
