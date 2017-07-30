package parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import model.Conditions;

public class QueryParser implements Cloneable {
	private List<String> cols = new ArrayList<>();
	private String path;
	private boolean hasAggregate;
	private List<Conditions> condition = new ArrayList<>();
	private List<String> operators = new ArrayList<>();
	private String grp_clause;
	private String ord_clause;
	private String typeOfQuery;
	private boolean isWhere;
	private boolean isOrderBy;
	private boolean isGroupBy;
	
	public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	public boolean isGroupBy() {
		return isGroupBy;
	}
	public boolean isOrderBy() {
		return isOrderBy;
	}
	public boolean isWhere() {
		return isWhere;
	}
	public String getTypeOfQuery() {
		return typeOfQuery;
	}
	public List<String> getCols() {
		return cols;
	}
	public String getPath() {
		return path;
	}
	public boolean isHasAggregate() {
		return hasAggregate;
	}
	public List<Conditions> getCondition() {
		return condition;
	}
	public List<String> getOperators() {
		return operators;
	}
	public String getGrp_clause() {
		return grp_clause;
	}
	public String getOrd_clause() {
		return ord_clause;
	}
	
	private void checkaggregate() {

		for (String col : cols) {
			if (col.contains("sum") || col.contains("count") || col.contains("avg") || col.contains("min")
					|| col.contains("max")) {
				typeOfQuery="aggregate";
				hasAggregate = true;
			} else
				hasAggregate = false;

		}
	}
	
	public QueryParser parseTheQuery(String query){
		
		String queryHolder = query;
		String splitQuery[];

		Pattern pat1 = Pattern.compile("select\\s.*from\\s.*");
		Matcher mat1 = pat1.matcher(queryHolder);
		if (mat1.find()) {
			if (queryHolder.contains("group by")) {
				isGroupBy=true;
				splitQuery = queryHolder.split("group by");
				grp_clause = splitQuery[1].replaceAll("\\s", "");
				queryHolder = splitQuery[0];

			}
			if (queryHolder.contains("order by")) {
				isOrderBy=true;
				splitQuery = queryHolder.split("order by");
				ord_clause = splitQuery[1].replaceAll("\\s", "");
				queryHolder = splitQuery[0];

			}
			if (queryHolder.contains("where")) {
				List<String> operands = new ArrayList<>();
				typeOfQuery="simple";
				isWhere=true;
				String wholeWhere = "";
				splitQuery = queryHolder.split("where");
				wholeWhere = splitQuery[1];

				String holder = wholeWhere;
				StringBuilder sb = new StringBuilder();
				for (String s : holder.split(" ")) {

					if (!s.equals("")) // ignore space
						sb.append(s + " "); // add word with 1 space

				}
				holder = sb.toString();
				sb.setLength(0);
				String splitHolder[] = holder.split(" ");
				int si = splitHolder.length;
				String splitNew[] = new String[si + 1];
				for (int s = 0; s < si; s++) {
					splitNew[s] = splitHolder[s];
				}
				splitNew[si] = "dummy";

				for (String st : splitNew) {
					if (!st.equals("and") && !st.equals("or") && !st.equals("dummy")) {
						sb.append(st);
					} else if (st.equals("and")) {
						operators.add("and");
						operands.add(sb.toString());
						sb.setLength(0);
					} else if (st.equals("or")) {
						operators.add("or");
						operands.add(sb.toString());
						sb.setLength(0);
					} else if (st.equals("dummy")) {
						operands.add(sb.toString());
						sb.setLength(0);
					}

				}

				int len = operands.size();
				Conditions conditionObj[] = new Conditions[len];
				for (int assign = 0; assign < len; assign++) {
					conditionObj[assign] = new Conditions();
					String conditionsplit[] = operands.get(assign).split("[\\s]*[>=|<=|!=|=|<|>][\\s]*");
					int l = conditionsplit.length;

					if (l == 3) {
						conditionObj[assign].setCol_name(conditionsplit[0]);
						conditionObj[assign].setValue(conditionsplit[2]);
						if (operands.get(assign).contains(">="))
							conditionObj[assign].setOperator(">=");
						if (operands.get(assign).contains("<="))
							conditionObj[assign].setOperator("<=");
						if (operands.get(assign).contains("!="))
							conditionObj[assign].setOperator("!=");
					} else if (l == 2) {
						conditionObj[assign].setCol_name(conditionsplit[0]);
						conditionObj[assign].setValue(conditionsplit[1]);
						if (operands.get(assign).contains(">"))
							conditionObj[assign].setOperator(">");
						if (operands.get(assign).contains("<"))
							conditionObj[assign].setOperator("<");
						if (operands.get(assign).contains("="))
							conditionObj[assign].setOperator("=");
					}
					condition.add(conditionObj[assign]);

				}
				queryHolder = splitQuery[0];
			}
			if (queryHolder.contains("from")) {
				typeOfQuery="simple";
				splitQuery = queryHolder.split("from");
				path = "F:\\DT-3\\" + splitQuery[1].replaceAll("\\s", "");
				queryHolder = splitQuery[0];
			}
			if (queryHolder.contains("select")) {
				splitQuery = queryHolder.split("select");
				String columns = splitQuery[1].replaceAll("\\s", "");
				if (!columns.equals("*")) {
					String splitedColname[] = columns.trim().split(",");
					for (int c = 0; c < splitedColname.length; c++) {
						cols.add(splitedColname[c]);
					}

					checkaggregate();
				} else {
					cols.add("*");
				}
				queryHolder = splitQuery[0];
				
			}
if(isGroupBy){
	typeOfQuery="groupby";
}
		}

		return this;
	}
	
	
}
