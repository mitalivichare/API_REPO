package com.processor.test;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.processor.Criteria;
import com.processor.QueryParser;

public class QueryParserTestCase {
	
	private static QueryParser queryParser,queryParser1;
	@BeforeClass
	public static void init()
	{
		queryParser=new QueryParser();
	}
	@Test
	public void tableNameTest()
	{
		queryParser.extractParameters("select * from emp");
		
		assertEquals("Table name test completed", "emp", queryParser.geTableName());
	}
	@Test
	public void multipleColumnTest()
	{
		queryParser.extractParameters("select empid,ename from emp");
		ArrayList<String> requiredColumnList=new ArrayList<>();
		requiredColumnList.add("empid");
		requiredColumnList.add("ename");
		assertEquals("Multiple column test completed", requiredColumnList, queryParser.getRequiredColumnList());
	}
	
	@Test
	public void groupByColumnTest()
	{
		queryParser.extractParameters("select empid,ename from emp group by empid");
		assertEquals("Group By column test completed", "empid", queryParser.getGroupByColumn());
	}
	
	@Test
	public void multipleWhereConditionTest()
	{
		queryParser.extractParameters("select empid,ename,esal from emp where empid > 102 and esal <= 20000");
		ArrayList<Criteria> criteriaList=new ArrayList<>();
		criteriaList.add(new Criteria("empid",">","102"));
		criteriaList.add(new Criteria("esal","<=","20000"));
		assertEquals("Multiple where condition test sucessfull", criteriaList.size(), queryParser.getCriteriaList().size());
	}
	
	/*@Test
	public void multipleColumnTestWithObject()
	{
		queryParser.extractParameters("select empid,ename,esal from emp where empid > 102 and esal <= 20000");
		queryParser1=new QueryParser();
		ArrayList<String> requiredColumnList=new ArrayList<>();
		requiredColumnList.add("empid");
		requiredColumnList.add("ename");
		requiredColumnList.add("esal");
		ArrayList<Criteria> criteriaList=new ArrayList<>();
		criteriaList.add(new Criteria("empid",">","102"));
		criteriaList.add(new Criteria("esal","<=","20000"));
		queryParser1.setCriteriaList(criteriaList);
		queryParser1.setRequiredColumnList(requiredColumnList);
		assertSame("multipleColumnTestWithObject", queryParser1, queryParser);
	}*/
	
	@AfterClass
	public static void destroy()
	{
		queryParser=null;
	
	}

}
