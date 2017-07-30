package parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import processor.GroupByProcessor;
import processor.AggregateProcessor;
import processor.QueryProcessor;
import processor.SimpleProcessor;

public class ProcessorController {
	QueryParser parser;
	BufferedReader reader;
	public Map<Integer,String> callProcessor(String query){
		QueryProcessor processor;
		parser=new QueryParser();
		Map<Integer,String> resultset=null;
		QueryParser parsedQuery=parser.parseTheQuery(query);
		switch(parsedQuery.getTypeOfQuery()){
		case "groupby":
			processor=new GroupByProcessor();
			resultset=processor.executeQuery(parsedQuery, getHeader(parsedQuery.getPath()));
		    break;
		case "aggregate":
			processor=new AggregateProcessor();
			resultset=processor.executeQuery(parsedQuery, getHeader(parsedQuery.getPath()));
			break;
		case "simple":
			processor=new SimpleProcessor();
			resultset=processor.executeQuery(parsedQuery, getHeader(parsedQuery.getPath()));
			break;
		}
		return resultset;
	}

	private Map<String, Integer> getHeader(String csvpath) {
		Map<String, Integer> header = new HashMap<>();
		try {
			reader = new BufferedReader(new FileReader(csvpath));
			String firstline = reader.readLine();
			String headerarr[] = firstline.split(",");
			int len = headerarr.length;
			for (int i = 0; i < len; i++) {
				header.put(headerarr[i], i);
			}
			

		} 
		catch (IOException io) {

		}

		return header;
	}
}
