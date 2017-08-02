package com.stackroute.datamunger.test;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.stackroute.datamunger.processor.QueryProcessor;

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
	
	//@Test
	public void selectAllWithoutWhereTestCase()
	{
		dataSet=queryProcessor.executeQuery("select * from emp");
		assertNotNull(dataSet);	
		display("selectAllWithoutWhereTestCase",dataSet);	
	}
	//@Test
	public void selectColumnsWithoutWhereTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,dept,name from emp");
		assertNotNull(dataSet);
		display("selectColumnWithoutWhereTestCase",dataSet);
		
	}
	
	//@Test
	public void withWhereGreaterThanTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp where salary > 30000");
		assertNotNull(dataSet);
		display("withWhereGreaterThanTestCase",dataSet);
		
	}
	
	//@Test
	public void withWhereLessThanTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp where salary < 35000");
		assertNotNull(dataSet);
		display("withWhereLessThanTestCase",dataSet);
		
	}
	
	//@Test
	public void withWhereLessThanOrEqualToTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp where salary <= 35000");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase",dataSet);
		
	}
	
	//@Test
	public void withWhereGreaterThanOrEqualToTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp where salary >= 35000");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase",dataSet);
		
	}
	
	//@Test
	public void withWhereNotEqualToTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp where salary >= 35000");
		assertNotNull(dataSet);
		display("withWhereNotEqualToTestCase",dataSet);
		
	}
	
	//@Test
	public void withWhereEqualAndNotEqualTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp where salary >= 30000 and salary <= 38000");
		assertNotNull(dataSet);
		display("withWhereEqualAndNotEqualTestCase",dataSet);
		
	}
	//@Test
	public void selectColumnsWithoutWhereWithOrderByTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp order by salary");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereWithOrderByTestCase",dataSet);
		
	}
	
	//@Test
	public void selectColumnsWithWhereWithOrderByTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,name,salary from emp where city = Bangalore order by salary");
		assertNotNull(dataSet);
		display("selectColumnsWithWhereWithOrderByTestCase",dataSet);
		
	}
	
	@Test
	public void simpleSumTestCase(){
			
			dataSet=queryProcessor.executeQuery("select sum(salary) from emp");
			assertNotNull(dataSet);
			display("simpleSumTestCase",dataSet);
			
		}
	@Test
	public void simpleCountTestCase(){
			
			dataSet=queryProcessor.executeQuery("select count(salary) from emp");
			assertNotNull(dataSet);
			display("simpleCountTestCase",dataSet);
			
		}
	
	@Test
	public void simpleAverageTestCase(){
			
			dataSet=queryProcessor.executeQuery("select avg(salary) from emp");
			assertNotNull(dataSet);
			display("simpleCountTestCase",dataSet);
			
		}
	@Test
	public void simpleMaxTestCase(){
			
			dataSet=queryProcessor.executeQuery("select max(salary) from emp");
			assertNotNull(dataSet);
			display("simpleMaxTestCase",dataSet);
			
		}
	
	@Test
	public void simpleMinTestCase(){
			
			dataSet=queryProcessor.executeQuery("select min(salary) from emp");
			assertNotNull(dataSet);
			display("simpleMinTestCase",dataSet);
			
		}
	
	//@Test
	public void selectColumnsWithoutWhereWithGroupByCountTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,count(*) from emp group by city");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereWithGroupByCountTestCase",dataSet);
		
	}
	
	//@Test
	public void selectColumnsWithoutWhereWithGroupBySumTestCase(){
		
		dataSet=queryProcessor.executeQuery("select city,sum(salary) from emp group by city");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereWithGroupBySumTestCase",dataSet);
		
	}
	
	private void display(String testCaseName,Map<Integer,ArrayList<String>> dataSet){
		System.out.println("================"+testCaseName+"==================");
		//System.out.println("================================================================");
		System.out.println(dataSet);
		//System.out.println("================================================================");
	}

}
