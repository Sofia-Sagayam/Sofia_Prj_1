package com.stackroute.datamunger.query.processor;

import java.util.Map;

import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.query.parser.Row;
import com.stackroute.datamunger.query.utilities.OrderData;
import com.stackroute.datamunger.query.utilities.QueryFetcher;

public class SimpleProcessor implements QueryProcessor {
	QueryFetcher dataset;

	@Override
	public Map<Long, Row> executeQuery(QueryParser parsedQuery) {
		dataset = new QueryFetcher();
		if (parsedQuery.getQueryParameter().getOrderByFeild() != null) {
			return new OrderData().orderingResultData(parsedQuery, dataset.getData(parsedQuery));
		}

		return dataset.getData(parsedQuery);
	}
}