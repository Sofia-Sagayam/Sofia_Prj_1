package com.stackroute.datamunger.query.processor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.query.parser.Row;
import com.stackroute.datamunger.query.utilities.Header;
import com.stackroute.datamunger.query.utilities.QueryFetcher;

public class GroupByProcessor implements QueryProcessor {

	

	@Override
	public Map<Long, Row> executeQuery(QueryParser parsedQuery) {
		QueryFetcher data = new QueryFetcher();
Header headerObject=new Header();
		Map<String, Integer> header = headerObject.headerMap(parsedQuery.getQueryParameter().getPath());
		Map<Long, Row> dataSet = new HashMap<>();
		Set<Row> rowSet = new HashSet<>();
		String fields = parsedQuery.getQueryParameter().getFeilds().get(0);
		String groupByColumn = parsedQuery.getQueryParameter().getGroupByFeild();
		if (fields.equals(groupByColumn)) {

			BufferedReader reader = null;
			String tuple = "";
			Row<String, String> rows;
			Long index = new Long(1);
			if (parsedQuery.getQueryParameter().getConditions().isEmpty()) {
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getQueryParameter().getPath()));
					String headers[] = reader.readLine().split(",");
					tuple = reader.readLine();
					int i = 0;
					while (tuple != null) {
						rows = new Row<>();
						String r[] = tuple.split(",");
						int length = r.length;
						rows.put(headers[header.get((groupByColumn))], r[header.get(groupByColumn)]);
						rowSet.add(rows);

						tuple = reader.readLine();

					}
					for (Row row : rowSet) {
						dataSet.put(index, row);
						index++;
					}
				} catch (IOException io) {

				}

			} else {
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getQueryParameter().getPath()));
					String headerValues[] = reader.readLine().split(",");
					tuple = reader.readLine();
					int i = 0;
					while (tuple != null) {
						rows = new Row<>();
						String r[] = tuple.split(",");
						int len = r.length;
						if (data.restrictionCheck(r, parsedQuery)) {
							rows.put(headerValues[header.get((groupByColumn))], r[header.get(groupByColumn)]);
							rowSet.add(rows);
						}
						tuple = reader.readLine();

					}
					for (Row row : rowSet) {
						dataSet.put(index, row);
						index++;
					}
				} catch (IOException io) {

				}
			}

		}

		return dataSet;
	}
}
