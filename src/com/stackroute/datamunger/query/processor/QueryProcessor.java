package com.stackroute.datamunger.query.processor;

import java.util.Map;

import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.query.parser.Row;

public interface QueryProcessor {
	public Map<Long, Row> executeQuery(QueryParser parsedQuery);
}
