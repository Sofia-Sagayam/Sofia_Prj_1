package com.stackroute.datamunger.query.utilities;

import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.stackroute.datamunger.query.parser.Row;

public class OrderByComparator implements Comparator<Row> {
	private String orderByColumn;

	public String getOrderByColumn() {
		return orderByColumn;
	}

	public void setOrderByColumn(String orderByColumn) {
		this.orderByColumn = orderByColumn;
	}

	@Override
	public int compare(Row row1, Row row2) {
		Pattern pattern = Pattern.compile("^\\d+$");
		Matcher mattern = pattern.matcher(row1.get(this.orderByColumn).toString());
		if (mattern.find()) {
			if (Integer.parseInt(row1.get(this.orderByColumn).toString()) < Integer
					.parseInt(row2.get(this.orderByColumn).toString())) {
				return 1;
			} else {
				return -1;
			}
		} else {
			if (row1.get(this.orderByColumn).toString().compareTo(row2.get(this.orderByColumn).toString()) < 0) {
				return -1;
			} else {
				return 1;
			}
		}
	}

}
