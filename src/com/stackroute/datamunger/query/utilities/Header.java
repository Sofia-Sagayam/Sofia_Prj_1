package com.stackroute.datamunger.query.utilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Header {
	public Map<String, Integer> headerMap(String csvpath) {
		Map<String, Integer> header = new HashMap<>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(csvpath));
			String firstline = reader.readLine();
			String headers[] = firstline.split(",");
			int headerLength = headers.length;
			for (int i = 0; i < headerLength; i++) {
				header.put(headers[i], i);
			}

		} catch (IOException io) {
			io.printStackTrace();
		}

		return header;
	}
}
