package tester;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.stackroute.datamunger.query.parser.Query;
import com.stackroute.datamunger.query.parser.Row;

public class QueryTest {
	Query controller;

	@Before
	public void instantiate() {
		controller = new Query();
	}

	
	/*@Test
	public void selectAllWithoutWhereTestCase() {

		Map<Long, Row> resultSet = controller.executeQuery("select * from F:\\DT-3\\CsvDB.csv");
		assertNotNull(resultSet);
		show("selectAllWithoutWhereTestCase", resultSet);

	}

	@Test
	public void selectColumnsWithoutWhereTestCase() {

		Map<Long, Row> dataSet = controller.executeQuery("select City,Dept,Name from F:\\DT-3\\CsvDB.csv");
		assertNotNull(dataSet);
		show("selectColumnsWithoutWhereTestCase", dataSet);

	}

	@Test
	public void withWhereGreaterThanTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select City,Name,Salary from F:\\DT-3\\CsvDB.csv where Salary>30000");
		assertNotNull(dataSet);
		show("withWhereGreaterThanTestCase", dataSet);

	}

	@Test
	public void withWhereLessThanTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select City,Name,Salary from F:\\DT-3\\CsvDB.csv where Salary<35000");
		assertNotNull(dataSet);
		show("withWhereLessThanTestCase", dataSet);

	}

	@Test
	public void withWhereLessThanOrEqualToTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select City,Name,Salary from F:\\DT-3\\CsvDB.csv where Salary<=35000");
		assertNotNull(dataSet);
		show("withWhereLessThanOrEqualToTestCase", dataSet);

	}

	@Test
	public void withWhereGreaterThanOrEqualToTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select City,Name,Salary from F:\\DT-3\\CsvDB.csv where Salary>=35000");
		assertNotNull(dataSet);
		show("withWhereLessThanOrEqualToTestCase", dataSet);

	}

	@Test
	public void withWhereNotEqualToTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select City,Name,Salary from F:\\DT-3\\CsvDB.csv where Salary!=35000");
		assertNotNull(dataSet);
		show("withWhereNotEqualToTestCase", dataSet);

	}

	@Test
	public void withWhereEqualAndNotEqualTestCase() {

		Map<Long,Row> dataSet = controller
				.executeQuery("select City,Name,Salary from F:\\DT-3\\CsvDB.csv where City=Bangalore AND Salary<=40000");
		assertNotNull(dataSet);
		show("withWhereEqualAndNotEqualTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithoutWhereWithOrderByTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select City,Name,Salary from F:\\DT-3\\CsvDB.csv order by Name");
		assertNotNull(dataSet);
		show("selectColumnsWithoutWhereWithOrderByTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithWhereWithOrderByTestCase() {

		Map<Long,Row> dataSet = controller
				.executeQuery("select City,Name,Salary from F:\\DT-3\\CsvDB.csv where City=Bangalore order by Name");
		assertNotNull(dataSet);
		show("selectColumnsWithWhereWithOrderByTestCase", dataSet);

	}

	@Test
	public void selectCountColumnsWithoutWhereTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select count(Name) from F:\\DT-3\\CsvDB.csv");
		assertNotNull(dataSet);
		show("selectCountColumnsWithoutWhereTestCase", dataSet);

	}

	@Test
	public void selectSumColumnsWithoutWhereTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select sum(Salary) from F:\\DT-3\\CsvDB.csv");
		assertNotNull(dataSet);
		show("selectSumColumnsWithoutWhereTestCase", dataSet);
	}

	@Test
	public void selectAvgColumnsWithoutWhereTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select avg(Salary) from F:\\DT-3\\CsvDB.csv");
		assertNotNull(dataSet);
		show("selectAvgColumnsWithoutWhereTestCase", dataSet);
	}

	@Test
	public void selectMinColumnsWithoutWhereTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select min(Salary) from F:\\DT-3\\CsvDB.csv");
		assertNotNull(dataSet);
		show("selectMinColumnsWithoutWhereTestCase", dataSet);
	}

	@Test
	public void selectMaxColumnsWithoutWhereTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select max(Salary) from F:\\DT-3\\CsvDB.csv");
		assertNotNull(dataSet);
		show("selectMaxColumnsWithoutWhereTestCase", dataSet);
	}

	@Test
	public void selectSumColumnsWithWhereTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select sum(Salary) from F:\\DT-3\\CsvDB.csv where City=Bangalore");
		assertNotNull(dataSet);
		show("selectSumColumnsWithWhereTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithoutWhereWithGroupByCountTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select City,count(Name) from F:\\DT-3\\CsvDB.csv group by City");
		assertNotNull(dataSet);
		show("selectColumnsWithoutWhereWithOrderByTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithoutWhereWithGroupBySumTestCase() {

		Map<Long,Row> dataSet = controller.executeQuery("select Name,Dept from F:\\DT-3\\CsvDB.csv where Salary>30000 AND City=Bangalore");
		assertNotNull(dataSet);
		show("selectColumnsWithoutWhereWithOrderByTestCase", dataSet);

	}
*/
	
	public void show(String msg, Map<Long, Row> resultSet) {
		System.out.println(msg + "\n" + "=================");
		for (Row s : resultSet.values())
			System.out.println(s);
	}
}