package com.stackroute.datamunger.processor;

import java.util.ArrayList;
import java.util.Map;

import com.stackroute.datamunger.parser.QueryParameter;
import com.stackroute.datamunger.reader.AggregateQuery;
import com.stackroute.datamunger.reader.SimpleQuery;

public class QueryProcessor 
{
	private SimpleQuery simpleQuery;
	private AggregateQuery aggregateQuery;
	Map<Integer,ArrayList<String>> dataSet;
	private QueryParameter queryParser;
	
	public QueryProcessor()
	{
		simpleQuery=new SimpleQuery();
		aggregateQuery=new AggregateQuery();
	}
	
	public Map<Integer,ArrayList<String>> executeQuery(String query)
	{
		queryParser=new QueryParameter();
		queryParser =queryParser.extractParameters(query);
		try
		{
			switch(queryParser.getQueryType())
			{
				case "SIMPLE_QUERY" :
					dataSet=simpleQuery.executeQuery(queryParser);
					break;
				
				case "AGGREGATE_QUERY" :
					dataSet=aggregateQuery.executeQuery(queryParser);
					break;
			
			}
		}
		catch(Exception e)
		{}
		return dataSet;
	}

}
