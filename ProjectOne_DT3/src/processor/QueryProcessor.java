package processor;

import java.util.Map;

import parser.QueryParser;

public interface QueryProcessor {
public Map<Integer,String> executeQuery(QueryParser parsedQuery,Map<String,Integer> header);
}
