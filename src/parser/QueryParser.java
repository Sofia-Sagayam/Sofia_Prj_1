package parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import model.Aggregate;
import model.Conditions;

public class QueryParser implements Cloneable {
	
	private String groupByFeild;
	private String orderByFeild;
	private String path;
	private String typeOfQuery;
	private	Map<String, Integer> header = new HashMap<>();
	private List<Conditions> conditions = new ArrayList<>();
	private List<String> logicalOperators = new ArrayList<>();
	private List<String> feilds = new ArrayList<>();
	private List<Aggregate> aggregates=new ArrayList<>();
	
	public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	public List<Conditions> getConditions() {
		return conditions;
	}

	public void setConditions(List<Conditions> conditions) {
		this.conditions = conditions;
	}

	public List<String> getLogicalOperators() {
		return logicalOperators;
	}

	public void setLogicalOperators(List<String> logicalOperators) {
		logicalOperators = logicalOperators;
	}

	public List<String> getFeilds() {
		return feilds;
	}

	public void setFeilds(List<String> feilds) {
		this.feilds = feilds;
	}

	public String getGroupByFeild() {
		return groupByFeild;
	}

	public void setGroupByFeild(String groupByFeild) {
		this.groupByFeild = groupByFeild;
	}

	public String getOrderByFeild() {
		return orderByFeild;
	}

	public void setOrderByFeild(String orderByFeild) {
		this.orderByFeild = orderByFeild;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	

	public String getTypeOfQuery() {
		return typeOfQuery;
	}

	public void setTypeOfQuery(String typeOfQuery) {
		this.typeOfQuery = typeOfQuery;
	}

	public Map<String, Integer> getHeader() {
		return header;
	}

	public void setHeader(Map<String, Integer> header) {
		this.header = header;
	}

	public List<Aggregate> getAggregates() {
		return aggregates;
	}

	public void setAggregates(List<Aggregate> aggregates) {
		this.aggregates = aggregates;
	}

	private void checkaggregate() {
			typeOfQuery="AGGREGATE";
int length=feilds.size();
Aggregate aggregateValue[]=new Aggregate[length];
		for (int f=0;f<length;f++) {
			aggregateValue[f]=new Aggregate();
			aggregateValue[f].setColumn(feilds.get(f).split("\\(")[1].trim().split("\\)")[0].trim());
            aggregateValue[f].setAggreFunc(feilds.get(f).split("\\(")[0].trim());
			}
			aggregates=Arrays.asList(aggregateValue);
		}
	
	
	public QueryParser parseTheQuery(String queryString){
	
		if (queryString.contains("group by")) {
			typeOfQuery="GROUP_BY";
			groupByFeild=queryString.split("group by")[1].trim().split("order by")[0].trim().split("\\s+")[0];
		}
		if(queryString.contains("order by")){
			orderByFeild=queryString.split("order by")[1].trim().split("\\s+")[0];
		}
		if(queryString.contains("where")){
			List<String> whereCondition = Arrays.asList(queryString.split("where")[1].trim().split("order by")[0].trim().split("group by")[0].trim().split("AND|OR"));
			int length=whereCondition.size();
			String logi[]=new String[length-1];
			for(int i=0;i<length-1;i++){
				logi[i]=new String();
				logi[i]=queryString.split(whereCondition.get(i))[1].trim().split(whereCondition.get(i+1))[0].trim();
			}
			logicalOperators=Arrays.asList(logi);
Conditions conditionObj[]=new Conditions[length];
			for(int i=0;i<length;i++)
			 {
				conditionObj[i]=new Conditions();
				conditionObj[i].setCol_name(whereCondition.get(i).split("<=|>=|!=|<|>|=")[0].trim());
				conditionObj[i].setValue(whereCondition.get(i).split("<=|>=|!=|<|>|=")[1].trim());
				conditionObj[i].setOperator(whereCondition.get(i).split(conditionObj[i].getColName())[1].split(conditionObj[i].getValue())[0].trim());
				  
			 }
			conditions=Arrays.asList(conditionObj);
			
			//if(queryString.contains("))
		}
			if (queryString.contains("from")) {
				typeOfQuery="SIMPLE";
				path =	queryString.split("from")[1].trim().split("\\s+")[0];
			}
			if (queryString.contains("select")) {
				String columns =queryString.split("from")[0].trim().split("select")[1];
				 
				if (!columns.equals("*")) {
					String splitedColname[] = columns.trim().split(",");
					for (int c = 0; c < splitedColname.length; c++) {
						feilds.add(splitedColname[c]);
					}
if(columns.contains("sum")||columns.contains("avg")||columns.contains("min")||columns.contains("max")||columns.contains("count")){
					checkaggregate();}
				} else {
					feilds.add("*");
				}
			}
if(groupByFeild!=null){
	typeOfQuery="GROUP_BY";
}
		headerMap(path);

		return this;
	}
	private void headerMap(String csvpath) {
		try {
	BufferedReader	reader = new BufferedReader(new FileReader(csvpath));
			String firstline = reader.readLine();
			String headerarr[] = firstline.split(",");
			int len = headerarr.length;
			for (int i = 0; i < len; i++) {
				header.put(headerarr[i], i);
			}
			

		} 
		catch (IOException io) {
io.printStackTrace();
		}

		
	}
	
}
