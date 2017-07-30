package processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import model.Aggregate;
import parser.QueryParser;
import utility.ReteriveData;

public class AggregateProcessor implements QueryProcessor {

	@Override
	public Map<Integer,String > executeQuery(QueryParser parsedQuery, Map<String, Integer> header) {
		Aggregate agg=new Aggregate();
int len=parsedQuery.getCols().size();
int index=0;
String aggrCol[]= new String[len];
String aggrFuc[]= new String[len];
for(int i=0;i<len;i++){
Pattern pat=Pattern.compile("\\((.*?)\\)");
Matcher mat=pat.matcher(parsedQuery.getCols().get(i));
if(mat.find()){
	
		aggrCol[i]=mat.group(1);
}
aggrFuc[i]=parsedQuery.getCols().get(i).split(aggrCol[i])[0];
	}

Map<Integer, String> filterDataToAggre = null;
	
try{	
QueryParser alter =(QueryParser) parsedQuery.clone();
for(int l=0;l<len;l++){
alter.getCols().set(l, aggrCol[l]);
}
filterDataToAggre = new ReteriveData().getDataFromCsv(alter,header);
}
catch(Exception e){}


Map<Integer, String> rowdata = new HashMap<>();
Map<Integer, Aggregate> dataset = new HashMap<>();

int sum[]=new int[len];
String str[]=new String[len];
for(int l=0;l<len;l++){
if (aggrFuc[l].contains("sum")) {
	
	for (String fil : filterDataToAggre.values()) {
		str[l]=fil;
		sum[l]+=Integer.parseInt(str[]);
			}
	agg.setAggreFunc("sum");
	agg.setColumn(aggrCol[l]);
	agg.setResult(sum[l]);
	dataset.put(++index, agg);
	
	}
	
//rowdata.put(index, sum);
index++;
}
	
int i=0;
for(Aggregate a:dataset.values()){
	rowdata.put(i,a.toString());
	i++;
}
return rowdata;
}

}
