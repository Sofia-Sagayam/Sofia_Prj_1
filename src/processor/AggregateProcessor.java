package processor;

import java.util.ArrayList;
import java.util.Arrays;
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
	public Map<Integer,String > executeQuery(QueryParser parsedQuery) {
		int len=parsedQuery.getFeilds().size();
		Aggregate agg=new Aggregate();
		int index=0;
		/*
int len=parsedQuery.getFeilds().size();

String aggrCol[]= new String[len];
String aggrFuc[]= new String[len];
for(int i=0;i<len;i++){
Pattern pattern=Pattern.compile("\\((.*?)\\)");
Matcher matcher=pattern.matcher(parsedQuery.getFeilds().get(i));
if(matcher.find()){
	
		aggrCol[i]=matcher.group(1);
}
aggrFuc[i]=parsedQuery.getFeilds().get(i).split(aggrCol[i])[0];
	}
*/
Map<Integer, String> filterDataToAggre = null;
	
try{	
QueryParser alter =(QueryParser) parsedQuery.clone();
alter.getFeilds().clear();
for(int l=0;l<len;l++){
alter.getFeilds().add(l, parsedQuery.getAggregates().get(l).getColumn());
}

	System.out.println(alter.getFeilds());

filterDataToAggre = new ReteriveData().getDataFromCsv(alter);
}
catch(CloneNotSupportedException e){
	e.printStackTrace();
}



Map<Integer, String> rowdata = new HashMap<>();
Map<Integer, Aggregate> dataset = new HashMap<>();

int sum[]=new int[len];
String str[]=new String[len];


for(int l=0;l<len;l++){
if (parsedQuery.getAggregates().get(l).getAggreFunc().contains("sum")) {
	
	for (String fil : filterDataToAggre.values()) {
		str[l]=fil.split(",")[0];
		sum[l]+=Integer.parseInt(str[l]);
			}
	
	agg.setResult(sum[l]);
	dataset.put(++index, agg);
	
	}
else if(parsedQuery.getAggregates().get(l).getAggreFunc().contains("avg")){
	
int cou=0;
	for (String fil : filterDataToAggre.values()) {
		str[l]=fil;
		sum[l]+=Integer.parseInt(str[l]);
		cou++;
	}
	agg.setResult(sum[l]/cou);
	dataset.put(++index, agg);
}
else if(parsedQuery.getAggregates().get(l).getAggreFunc().contains("count")){

	agg.setResult(filterDataToAggre.size());
	dataset.put(++index, agg);
}
else if(parsedQuery.getAggregates().get(l).getAggreFunc().contains("min")){

	String minArray[]=filterDataToAggre.values().toArray(new String[0]);
		Arrays.sort(minArray);	
		agg.setResult(Integer.parseInt(minArray[0]));
	dataset.put(++index, agg);
}
else if(parsedQuery.getAggregates().get(l).getAggreFunc().contains("max")){
	String maxArray[]=filterDataToAggre.values().toArray(new String[0]);
	Arrays.sort(maxArray);
	agg.setResult(Integer.parseInt(maxArray[maxArray.length-1]));
	dataset.put(++index, agg);
}
//rowdata.put(index, sum);
//index++;
}
	
int i=0;
for(Aggregate a:dataset.values()){
	rowdata.put(i,a.toString());
	i++;
}
return rowdata;
}

}
