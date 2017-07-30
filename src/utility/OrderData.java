package utility;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parser.QueryParser;

public class OrderData {
	private Map<String,Integer> header;
	public Map<Integer,String> orderingResultData(QueryParser parser,Map<Integer,String> dataset){
		BufferedReader reader;
		header=parser.getHeader();
		Map<Integer, String> rowData = new HashMap<>();
		try {
			reader = new BufferedReader(new FileReader(parser.getPath()));

			int index = 0;
			String str = reader.readLine();
			str = reader.readLine();
			List<String> col_of_orderby = new ArrayList<>();
			String afterSplit[]=null;
			while (str != null) {
				afterSplit = str.split(",");
				col_of_orderby.add(afterSplit[header.get(parser.getOrderByFeild())]);
				str = reader.readLine();
			}

			Collections.sort(col_of_orderby);
			reader.close();

			int l = col_of_orderby.size();

			
			for (int k = 0; k < l; k++) {
				try{
				for(Map.Entry<Integer,String> entry: dataset.entrySet()){
					if(entry.getValue().contains(col_of_orderby.get(k))){
						rowData.put(index, entry.getValue());
						index++;
						dataset.remove(entry.getKey());
					}
				}
				}
				catch(ConcurrentModificationException cee){
					cee.printStackTrace();
				}
			}
		}

		catch (IOException io) {
			io.printStackTrace();

		}

		return rowData;
	}

}
