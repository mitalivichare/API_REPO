package com.processor.test;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.processor.QueryProcessor;

public class QueryProcessorTestCase 
{
	private static QueryProcessor queryProcessor;
	private static Map<Integer,ArrayList<String>> dataSet;
	
	@BeforeClass
	public static void init()
	{
		queryProcessor=new QueryProcessor();
		dataSet=new LinkedHashMap<Integer,ArrayList<String>>();
	}
	
	/*@Test
	public void selectAllWithoutWhereTestCase()
	{
		dataSet=queryProcessor.executeQuery("select * from emp");
		assertNotNull(dataSet);	
		display("selectAllWithoutWhereTestCase",dataSet);	
	}*/
	/*@Test
	public void selectColumnsWithoutWhereTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,dept,name from emp");
		assertNotNull(dataSet);
		display("selectColumnWithoutWhereTestCase",dataSet);
		
	}*/
	
	/*@Test
	public void withWhereGreaterThanTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp where salary > 30000");
		assertNotNull(dataSet);
		display("withWhereGreaterThanTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereLessThanTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp where salary < 35000");
		assertNotNull(dataSet);
		display("withWhereLessThanTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereLessThanOrEqualToTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp where salary <= 35000");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereGreaterThanOrEqualToTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp where salary >= 35000");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereNotEqualToTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp where salary >= 35000");
		assertNotNull(dataSet);
		display("withWhereNotEqualToTestCase",dataSet);
		
	}*/
	
	@Test
	public void withWhereEqualAndNotEqualTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp where salary >= 30000 and salary <= 38000");
		assertNotNull(dataSet);
		display("withWhereEqualAndNotEqualTestCase",dataSet);
		
	}
	
	private void display(String testCaseName,Map<Integer,ArrayList<String>> dataSet){
		System.out.println(testCaseName);
		System.out.println("================================================================");
		System.out.println(dataSet);
	}

}
