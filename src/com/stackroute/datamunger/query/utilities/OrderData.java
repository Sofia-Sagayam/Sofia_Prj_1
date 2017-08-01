package com.stackroute.datamunger.query.utilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.query.parser.Row;

public class OrderData {
	public Map<Long, Row> orderingResultData(QueryParser parser, Map<Long, Row> dataset) {
		List<Row> rows = new ArrayList<>(dataset.values());
		OrderByComparator sorting = new OrderByComparator();
		sorting.setOrderByColumn(parser.getQueryParameter().getOrderByFeild());
		Collections.sort(rows, sorting);
		Map<Long, Row> dataSet = new LinkedHashMap<>();
		Long i = new Long(0);
		for (Row row : rows) {
			dataSet.put(++i, row);
		}
		return dataSet;
	}
}
