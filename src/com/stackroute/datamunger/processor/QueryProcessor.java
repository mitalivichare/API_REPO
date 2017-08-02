package com.stackroute.datamunger.processor;

import java.util.ArrayList;
import java.util.Map;

public class QueryProcessor 
{
	private Query queryObject;
	Map<Integer,ArrayList<String>> dataSet;
	private QueryParameter queryParser;
	
	public QueryProcessor()
	{
		/*simpleQuery=new SimpleQuery();
		aggregateQuery=new AggregateQuery();*/
	}
	
	public Map<Integer,ArrayList<String>> executeQuery(String query)
	{
		queryParser=new QueryParameter();
		queryParser =queryParser.extractParameters(query);
		try
		{
			switch(queryParser.getQueryType())
			{
				case "SIMPLE_QUERY":
					queryObject=new SimpleQueryProcessor();
					break;
					
				case "WHERE_QUERY" :
					queryObject=new WhereQueryProcessor();
					break;
					
				case "ORDER_BY_QUERY" :
					queryObject=new OrderByQueryProcessor();
					break;
					
				case "GROUP_BY_QUERY" :
					queryObject=new GroupByQueryProcessor();
					break;
				
				case "AGGREGATE_QUERY" :
					queryObject=new AggregateQueryProcessor();
					break;
			
			}
			dataSet=queryObject.executeQuery(queryParser);
		}
		catch(Exception e)
		{}
		return dataSet;
	}

}
