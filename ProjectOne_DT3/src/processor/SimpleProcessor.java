package processor;

import java.util.Map;

import parser.QueryParser;
import utility.OrderData;
import utility.ReteriveData;



public class SimpleProcessor implements QueryProcessor {
ReteriveData dataset;
	@Override
	public Map<Integer, String> executeQuery(QueryParser parsedQuery, Map<String, Integer> header) {
	dataset=new ReteriveData();
	if(parsedQuery.isOrderBy()){
		return new OrderData().orderingResultData(parsedQuery,dataset.getDataFromCsv(parsedQuery, header),header);	
		}

	return dataset.getDataFromCsv(parsedQuery, header);
	}
}