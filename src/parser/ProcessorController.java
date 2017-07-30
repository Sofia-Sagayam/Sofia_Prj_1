package parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
		Map<Integer,String> resultSet=null;

		Pattern pattern = Pattern.compile("select\\s.*from\\s.*");
		Matcher mattern = pattern.matcher(query);
		if(mattern.find()){

		QueryParser parsedQuery=parser.parseTheQuery(query);
		switch(parsedQuery.getTypeOfQuery()){
		case "GROUP_BY":
			processor=new GroupByProcessor();
			resultSet=processor.executeQuery(parsedQuery);
		    break;
		case "AGGREGATE":
			processor=new AggregateProcessor();
			resultSet=processor.executeQuery(parsedQuery);
			break;
		case "SIMPLE":
			processor=new SimpleProcessor();
			resultSet=processor.executeQuery(parsedQuery);
			break;
		}
		}
		return resultSet;
	}

	
}
