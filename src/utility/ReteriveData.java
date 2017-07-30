package utility;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import parser.QueryParser;

public class ReteriveData {
	private Map<String,Integer> header;
	private boolean restrictionCheck(String aftersplit[],QueryParser parsedQuery){
		header=parsedQuery.getHeader();
		int logiOperLen = parsedQuery.getLogicalOperators().size() + 1;
		boolean mulRes[] = new boolean[logiOperLen];
		for (int logOp = 0; logOp < logiOperLen; logOp++) {
			if (parsedQuery.getConditions().get(logOp).getOperator().equals("=")) {
				mulRes[logOp] = aftersplit[header.get(parsedQuery.getConditions().get(logOp).getColName())].equals(parsedQuery.getConditions().get(logOp).getValue());
			} else if (parsedQuery.getConditions().get(logOp).getOperator().equals("!=")) {
				mulRes[logOp] = !(aftersplit[header.get(parsedQuery.getConditions().get(logOp).getColName())].equals(parsedQuery.getConditions().get(logOp).getValue()));
			} else if (parsedQuery.getConditions().get(logOp).getOperator().equals(">")) {
				long res = Math.max(Long.parseLong(aftersplit[header.get(parsedQuery.getConditions().get(logOp).getColName())]),Long.parseLong(parsedQuery.getConditions().get(logOp).getValue()));
				mulRes[logOp] = Long.toString(res).equals(aftersplit[header.get(parsedQuery.getConditions().get(logOp).getColName())])&& !Long.toString(res).equals(parsedQuery.getConditions().get(logOp).getValue());
			} else if (parsedQuery.getConditions().get(logOp).getOperator().equals("<")) {
				long res = Math.min(Long.parseLong(aftersplit[header.get(parsedQuery.getConditions().get(logOp).getColName())]),Long.parseLong(parsedQuery.getConditions().get(logOp).getValue()));
				mulRes[logOp] = Long.toString(res).equals(aftersplit[header.get(parsedQuery.getConditions().get(logOp).getColName())])&& !Long.toString(res).equals(parsedQuery.getConditions().get(logOp).getValue());

			} else if (parsedQuery.getConditions().get(logOp).getOperator().equals(">=")) {
				long res = Math.max(Long.parseLong(aftersplit[header.get(parsedQuery.getConditions().get(logOp).getColName())]),Long.parseLong(parsedQuery.getConditions().get(logOp).getValue()));
				mulRes[logOp] = Long.toString(res).equals(aftersplit[header.get(parsedQuery.getConditions().get(logOp).getColName())]);
			} else if (parsedQuery.getConditions().get(logOp).getOperator().equals("<=")) {
				long res = Math.min(Long.parseLong(aftersplit[header.get(parsedQuery.getConditions().get(logOp).getColName())]),Long.parseLong(parsedQuery.getConditions().get(logOp).getValue()));
				mulRes[logOp] = Long.toString(res).equals(aftersplit[header.get(parsedQuery.getConditions().get(logOp).getColName())]);

			}
		}
		boolean res = mulRes[0];
		for (int god = 0; god < logiOperLen - 1; god++) {

			if (parsedQuery.getLogicalOperators().get(god).equals("and")) {
				res = res && mulRes[god + 1];
			} else if (parsedQuery.getLogicalOperators().get(god).equals("or")) {
				res = res || mulRes[god + 1];
			}
		}
		return res;
	}
	
	public Map<Integer,String> getDataFromCsv(QueryParser parsedQuery){
		header=parsedQuery.getHeader();
		Map<Integer, String> rowdata = new HashMap<>();
		BufferedReader reader = null;
		int index = 0, h = 0;
		int colcount = 0;
		String str = "";
		String aftersplit[];
		if(parsedQuery.getFeilds().get(0).equals("*")){
			if(!parsedQuery.getConditions().isEmpty()){
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getPath()));
					colcount = parsedQuery.getFeilds().size();
					str = reader.readLine();
					str = reader.readLine();
					
					
					while (str != null) {
						aftersplit = str.split(",");
						
				if (restrictionCheck(aftersplit, parsedQuery)) {
					rowdata.put(index, str);	
				}
				index++;
				str = reader.readLine();

				}

				}
				catch(IOException io)
				{
					io.printStackTrace();
				}

			}
			else{
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getPath()));
					colcount = parsedQuery.getFeilds().size();
					str = reader.readLine();
					str = reader.readLine();
					
					while (str != null) {
						rowdata.put(index, str);

						index++;
						str = reader.readLine();

					}
					}
				catch(IOException io){
					io.printStackTrace();

				}
			}
			
		}
		else{
			if(!parsedQuery.getConditions().isEmpty()){
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getPath()));
					colcount = parsedQuery.getFeilds().size();
					str = reader.readLine();
					str = reader.readLine();
					while (str != null) {
						aftersplit = str.split(",");
						
				if (restrictionCheck(aftersplit, parsedQuery)) {
					StringBuffer sb = new StringBuffer();
					for (h = 0; h < colcount; h++) {
						sb.append(aftersplit[header.get(parsedQuery.getFeilds().get(h))] + ",");
						String st = sb.toString();
						rowdata.put(index, st.substring(0, st.length() - 1));
					}
				}
				index++;
				str = reader.readLine();

				}

				}
				catch(IOException io){
					io.printStackTrace();

				}
		}
			else{
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getPath()));
					colcount = parsedQuery.getFeilds().size();
					str = reader.readLine();
					str = reader.readLine();
					
					while (str != null) {
						aftersplit = str.split(",");
						StringBuffer sb = new StringBuffer();
						for (h = 0; h < colcount; h++) {
							sb.append(aftersplit[header.get(parsedQuery.getFeilds().get(h))] + ",");
							String st = sb.toString();
							rowdata.put(index, st.substring(0, st.length() - 1));
						}
						index++;
						str = reader.readLine();
					}
				}
				catch(IOException io){
				io.printStackTrace();
				}
			}
		}
		return rowdata;
	}
}