package com.stackroute.datamunger.query.parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.stackroute.datamunger.query.processor.AggregateProcessor;
import com.stackroute.datamunger.query.processor.GroupByAggregateProcessor;
import com.stackroute.datamunger.query.processor.GroupByProcessor;
import com.stackroute.datamunger.query.processor.QueryProcessor;
import com.stackroute.datamunger.query.processor.SimpleProcessor;

public class Query {
	QueryParser parser;

	public Map<Long, Row> executeQuery(String query) {
		QueryProcessor processor;
		parser = new QueryParser();
		Map<Long, Row> resultSet = null;

		Pattern pattern = Pattern.compile("select\\s.*from\\s.*");
		Matcher matcher = pattern.matcher(query);
		if (matcher.find()) {

			QueryParser parsedQuery = parser.parseQuery(query);
			switch (parsedQuery.getTypeOfQuery()) {
			case "GROUP_BY":
				processor = new GroupByProcessor();
				resultSet = processor.executeQuery(parsedQuery);
				break;
			case "AGGREGATE":
				processor = new AggregateProcessor();
				resultSet = processor.executeQuery(parsedQuery);
				break;
			case "SIMPLE":
				processor = new SimpleProcessor();
				resultSet = processor.executeQuery(parsedQuery);
				break;

			case "GROUP_BY_AGGREGATE":
				processor = new GroupByAggregateProcessor();
				resultSet = processor.executeQuery(parsedQuery);
				break;

			}
		}
		return resultSet;
	}

}
