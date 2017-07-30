package tester;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import parser.ProcessorController;

public class QueryTest {
ProcessorController controller;

@Before
public void instantiate(){
	controller=new ProcessorController();
}
@Test
public void sampleTest(){
	Map<Integer,String> resultSet=controller.callProcessor("select sum(Salary),sum(Empid) from CsvDB.csv");
	assertNotNull(resultSet);
	show("test", resultSet);
}

public void show(String msg,Map<Integer,String> resultSet){
System.out.println(msg+"\n"+"=================");
for(String s:resultSet.values())
System.out.println(s);
}
}