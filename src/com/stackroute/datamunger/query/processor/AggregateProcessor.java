package com.stackroute.datamunger.query.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.stackroute.datamunger.query.parser.Aggregate;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.query.parser.Row;
import com.stackroute.datamunger.query.utilities.QueryFetcher;

public class AggregateProcessor implements QueryProcessor {

	@Override
	public Map<Long, Row> executeQuery(QueryParser parsedQuery) {
		Map<Long, Row> dataSet = new HashMap<>();
		Map<Long, Row> rowSet = null;
		Long index = new Long(1);
		int noOfAggregate = parsedQuery.getQueryParameter().getAggregates().size();

		try {
			QueryParameter queryParameter = (QueryParameter) parsedQuery.getQueryParameter().clone();
			queryParameter.getFeilds().clear();
			for (int l = 0; l < noOfAggregate; l++) {
				queryParameter.getFeilds().add(l, parsedQuery.getQueryParameter().getAggregates().get(l).getColumn());
			}

			parsedQuery.setQueryParameter(queryParameter);
			rowSet = new QueryFetcher().getData(parsedQuery);
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		List<Row> rowList = new ArrayList<>(rowSet.values());
		String aggregateFunc = "";
		String aggregateColu = "";
		for (int a = 0; a < noOfAggregate; a++) {
			aggregateFunc = parsedQuery.getQueryParameter().getAggregates().get(a).getAggreFunc();
			aggregateColu = parsedQuery.getQueryParameter().getAggregates().get(a).getColumn();
			switch (aggregateFunc) {
			case "sum":
				int add = 0;
				for (Row row : rowList) {
					add += Integer.parseInt(row.get(aggregateColu).toString());
				}
				parsedQuery.getQueryParameter().getAggregates().get(a).setResult(add);
				break;
			case "avg":
				int total = 0;
				int count = 0;
				for (Row row : rowList) {
					total += Integer.parseInt(row.get(aggregateColu).toString());
					count++;
				}
				parsedQuery.getQueryParameter().getAggregates().get(a).setResult(total / count);
				break;
			case "min":
				int minInteger = Integer.parseInt(rowList.get(0).get(aggregateColu).toString());
				for (int i = 1; i < rowList.size(); i++) {
					if (Integer.parseInt(rowList.get(i).get(aggregateColu).toString()) < minInteger)
						minInteger = Integer.parseInt(rowList.get(i).get(aggregateColu).toString());
				}
				parsedQuery.getQueryParameter().getAggregates().get(a).setResult(minInteger);
				break;
			case "max":
				int maxInteger = Integer.parseInt(rowList.get(0).get(aggregateColu).toString());
				for (int i = 1; i < rowList.size(); i++) {
					if (Integer.parseInt(rowList.get(i).get(aggregateColu).toString()) > maxInteger)
						maxInteger = Integer.parseInt(rowList.get(i).get(aggregateColu).toString());
				}
				parsedQuery.getQueryParameter().getAggregates().get(a).setResult(maxInteger);
				break;
			case "count":
				parsedQuery.getQueryParameter().getAggregates().get(a).setResult(rowList.size());
				break;

			}
			Row row = new Row();
			row.put(aggregateFunc + "(" + aggregateColu + ")",
					parsedQuery.getQueryParameter().getAggregates().get(a).getResult());
			dataSet.put(index++, row);

		}

		return dataSet;
	}

}
