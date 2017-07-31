package com.stackroute.datamunger.processor;

import java.util.ArrayList;
import java.util.Map;

import com.stackroute.datamunger.parser.QueryParameter;
import com.stackroute.datamunger.reader.AggregateTypeReader;
import com.stackroute.datamunger.reader.GroupByTypeReader;
import com.stackroute.datamunger.reader.OrderByTypeReader;
import com.stackroute.datamunger.reader.SimpleTypeReader;
import com.stackroute.datamunger.reader.WhereTypeReader;

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
					queryObject=new SimpleTypeReader();
					break;
					
				case "WHERE_QUERY" :
					queryObject=new WhereTypeReader();
					break;
					
				case "ORDER_BY_QUERY" :
					queryObject=new OrderByTypeReader();
					break;
					
				case "GROUP_BY_QUERY" :
					queryObject=new GroupByTypeReader();
					break;
				
				case "AGGREGATE_QUERY" :
					queryObject=new AggregateTypeReader();
					break;
			
			}
			dataSet=queryObject.executeQuery(queryParser);
		}
		catch(Exception e)
		{}
		return dataSet;
	}

}
