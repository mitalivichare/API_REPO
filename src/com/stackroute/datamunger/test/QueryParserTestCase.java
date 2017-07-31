package com.stackroute.datamunger.test;

import static org.junit.Assert.*;

import java.util.ArrayList;

import javax.management.Query;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.stackroute.datamunger.parser.AggregateFunctions;
import com.stackroute.datamunger.parser.Criteria;
import com.stackroute.datamunger.parser.QueryParameter;

public class QueryParserTestCase {
	
	private static QueryParameter queryParser;
	@Before
	public void init()
	{
		queryParser=new QueryParameter();
	}
	//@Test
	public void tableNameTest()
	{
		queryParser.extractParameters("select * from emp");
		
		assertEquals("Table name test completed", "emp", queryParser.geTableName());
	}
	//@Test
	public void multipleColumnTest()
	{
		queryParser.extractParameters("select empid,ename from emp");
		ArrayList<String> requiredColumnList=new ArrayList<>();
		requiredColumnList.add("empid");
		requiredColumnList.add("ename");
		assertEquals("Multiple column test completed", requiredColumnList, queryParser.getRequiredColumnList());
	}
	
	//@Test
	public void groupByColumnTest()
	{
		queryParser.extractParameters("select empid,ename from emp group by empid");
		assertEquals("Group By column test completed", "empid", queryParser.getGroupByColumn());
	}
	

	//@Test
	public void orderByColumnTest()
	{
		queryParser.extractParameters("select empid,ename from emp order by empid");
		assertEquals("Group By column test completed", "empid", queryParser.getOrderByColumn());
	}
	
	@Test
	public void multipleWhereConditionTest()
	{
		queryParser.extractParameters("select empid,ename,esal from emp where empid > 102 and esal <= 20000");
		ArrayList<Criteria> criteriaList=new ArrayList<>();
		criteriaList.add(new Criteria("empid",">","102"));
		criteriaList.add(new Criteria("esal","<=","20000"));
		assertEquals("Multiple where condition test sucessfull", criteriaList.size(), queryParser.getCriteriaList().size());
		display("multipleWhereConditionTest", queryParser);
	}
	
	//@Test
	public void countColumnTest()
	{
		queryParser.extractParameters("select count(empid),sum(esal) from emp");
		ArrayList<AggregateFunctions> aggregateFunctionList=new ArrayList<>();
		aggregateFunctionList.add(new AggregateFunctions("count","empid",null));
		aggregateFunctionList.add(new AggregateFunctions("sum","esal",null));
		assertEquals("Count column test sucessfull", aggregateFunctionList.size(), queryParser.getAggregateFunctionsList().size());
		display("countColumnTest", queryParser);
	}
	
	//@Test
	public void sumColumnTest()
	{
		queryParser.extractParameters("select sum(esal) from emp");
		ArrayList<AggregateFunctions> aggregateFunctionList=new ArrayList<>();
		//aggregateFunctionList.add(new AggregateFunctions("count","empid",null));
		aggregateFunctionList.add(new AggregateFunctions("sum","esal",null));
		assertEquals("Count column test sucessfull", aggregateFunctionList.size(), queryParser.getAggregateFunctionsList().size());
		display("sumColumnTest", queryParser);
	}
	@Test
	public void sumColumnWithWhereTest()
	{
		queryParser.extractParameters("select sum(esal) from emp where city=Bangalore");
		ArrayList<AggregateFunctions> aggregateFunctionList=new ArrayList<>();
		//aggregateFunctionList.add(new AggregateFunctions("count","empid",null));
		aggregateFunctionList.add(new AggregateFunctions("sum","esal",null));
		assertEquals("Count column test sucessfull", aggregateFunctionList.size(), queryParser.getAggregateFunctionsList().size());
		display("sumColumnWithWhereTest", queryParser);
	}
		
	@AfterClass
	public static void destroy()
	{
		queryParser=null;
	}
	
	public void display(String testCaseName,QueryParameter queryParser)
	{
		System.out.println("============"+testCaseName+"==============");
		System.out.println(queryParser);
	}
	

}
