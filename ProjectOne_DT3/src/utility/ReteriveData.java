package utility;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import parser.QueryParser;

public class ReteriveData {
	
	private boolean restrictionCheck(String aftersplit[],QueryParser parsedQuery,Map<String,Integer> header){
		int logiOperLen = parsedQuery.getOperators().size() + 1;
		boolean mulRes[] = new boolean[logiOperLen];
		for (int logOp = 0; logOp < logiOperLen; logOp++) {
			if (parsedQuery.getCondition().get(logOp).getOperator().equals("=")) {
				mulRes[logOp] = aftersplit[header.get(parsedQuery.getCondition().get(logOp).getCol_name())].equals(parsedQuery.getCondition().get(logOp).getValue());
			} else if (parsedQuery.getCondition().get(logOp).getOperator().equals("!=")) {
				mulRes[logOp] = !(aftersplit[header.get(parsedQuery.getCondition().get(logOp).getCol_name())].equals(parsedQuery.getCondition().get(logOp).getValue()));
			} else if (parsedQuery.getCondition().get(logOp).getOperator().equals(">")) {
				long res = Math.max(Long.parseLong(aftersplit[header.get(parsedQuery.getCondition().get(logOp).getCol_name())]),Long.parseLong(parsedQuery.getCondition().get(logOp).getValue()));
				mulRes[logOp] = Long.toString(res).equals(aftersplit[header.get(parsedQuery.getCondition().get(logOp).getCol_name())])&& !Long.toString(res).equals(parsedQuery.getCondition().get(logOp).getValue());
			} else if (parsedQuery.getCondition().get(logOp).getOperator().equals("<")) {
				long res = Math.min(Long.parseLong(aftersplit[header.get(parsedQuery.getCondition().get(logOp).getCol_name())]),Long.parseLong(parsedQuery.getCondition().get(logOp).getValue()));
				mulRes[logOp] = Long.toString(res).equals(aftersplit[header.get(parsedQuery.getCondition().get(logOp).getCol_name())])&& !Long.toString(res).equals(parsedQuery.getCondition().get(logOp).getValue());

			} else if (parsedQuery.getCondition().get(logOp).getOperator().equals(">=")) {
				long res = Math.max(Long.parseLong(aftersplit[header.get(parsedQuery.getCondition().get(logOp).getCol_name())]),Long.parseLong(parsedQuery.getCondition().get(logOp).getValue()));
				mulRes[logOp] = Long.toString(res).equals(aftersplit[header.get(parsedQuery.getCondition().get(logOp).getCol_name())]);
			} else if (parsedQuery.getCondition().get(logOp).getOperator().equals("<=")) {
				long res = Math.min(Long.parseLong(aftersplit[header.get(parsedQuery.getCondition().get(logOp).getCol_name())]),Long.parseLong(parsedQuery.getCondition().get(logOp).getValue()));
				mulRes[logOp] = Long.toString(res).equals(aftersplit[header.get(parsedQuery.getCondition().get(logOp).getCol_name())]);

			}
		}
		boolean res = mulRes[0];
		for (int god = 0; god < logiOperLen - 1; god++) {

			if (parsedQuery.getOperators().get(god).equals("and")) {
				res = res && mulRes[god + 1];
			} else if (parsedQuery.getOperators().get(god).equals("or")) {
				res = res || mulRes[god + 1];
			}
		}
		return res;
	}
	
	public Map<Integer,String> getDataFromCsv(QueryParser parsedQuery, Map<String, Integer> header){
		Map<Integer, String> rowdata = new HashMap<>();
		BufferedReader reader = null;
		int index = 0, h = 0;
		int colcount = 0;
		String str = "";
		String aftersplit[];
		if(parsedQuery.getCols().get(0).equals("*")){
			if(parsedQuery.isWhere()){
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getPath()));
					colcount = parsedQuery.getCols().size();
					str = reader.readLine();
					str = reader.readLine();
					
					
					while (str != null) {
						aftersplit = str.split(",");
						
				if (restrictionCheck(aftersplit, parsedQuery, header)) {
					rowdata.put(index, str);	
				}
				index++;
				str = reader.readLine();

				}

				}
				catch(IOException io)
				{
					
				}

			}
			else{
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getPath()));
					colcount = parsedQuery.getCols().size();
					str = reader.readLine();
					str = reader.readLine();
					
					while (str != null) {
						rowdata.put(index, str);

						index++;
						str = reader.readLine();

					}
					}
				catch(IOException io){}
			}
			
		}
		else{
			if(parsedQuery.isWhere()){
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getPath()));
					colcount = parsedQuery.getCols().size();
					str = reader.readLine();
					str = reader.readLine();
					while (str != null) {
						aftersplit = str.split(",");
						
				if (restrictionCheck(aftersplit, parsedQuery, header)) {
					StringBuffer sb = new StringBuffer();
					for (h = 0; h < colcount; h++) {
						sb.append(aftersplit[header.get(parsedQuery.getCols().get(h))] + ",");
						String st = sb.toString();
						rowdata.put(index, st.substring(0, st.length() - 1));
					}
				}
				index++;
				str = reader.readLine();

				}

				}
				catch(IOException io){}
		}
			else{
				try {
					reader = new BufferedReader(new FileReader(parsedQuery.getPath()));
					colcount = parsedQuery.getCols().size();
					str = reader.readLine();
					str = reader.readLine();
					
					while (str != null) {
						aftersplit = str.split(",");
						StringBuffer sb = new StringBuffer();
						for (h = 0; h < colcount; h++) {
							sb.append(aftersplit[header.get(parsedQuery.getCols().get(h))] + ",");
							String st = sb.toString();
							rowdata.put(index, st.substring(0, st.length() - 1));
						}
						index++;
						str = reader.readLine();
					}
				}
				catch(IOException io){}
			}
		}
		return rowdata;
	}
}