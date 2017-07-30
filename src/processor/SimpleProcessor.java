package processor;

import java.util.Map;

import parser.QueryParser;
import utility.OrderData;
import utility.ReteriveData;



public class SimpleProcessor implements QueryProcessor {
ReteriveData dataset;
	@Override
	public Map<Integer, String> executeQuery(QueryParser parsedQuery) {
	dataset=new ReteriveData();
	if(parsedQuery.getOrderByFeild()!=null){
		return new OrderData().orderingResultData(parsedQuery,dataset.getDataFromCsv(parsedQuery));	
		}

	return dataset.getDataFromCsv(parsedQuery);
	}
}