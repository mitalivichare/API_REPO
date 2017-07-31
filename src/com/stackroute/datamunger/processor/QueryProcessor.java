package com.stackroute.datamunger.processor;

import java.util.ArrayList;
import java.util.Map;

import com.stackroute.datamunger.parser.QueryParameter;
import com.stackroute.datamunger.reader.AggregateQuery;
import com.stackroute.datamunger.reader.SimpleQuery;
import com.stackroute.datamunger.reader.WhereQueryProcessor;

public class QueryProcessor 
{
	private SimpleQuery simpleQuery;
	private WhereQueryProcessor whereProcessor;
	private AggregateQuery aggregateQuery;
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
					queryObject=new SimpleQuery();
					//dataSet=simpleQuery.executeQuery(queryParser);
					break;
					
				case "WHERE_QUERY" :
					queryObject=new WhereQueryProcessor();
					break;
				
				case "AGGREGATE_QUERY" :
					queryObject=new AggregateQuery();
					break;
			
			}
			dataSet=queryObject.executeQuery(queryParser);
		}
		catch(Exception e)
		{}
		return dataSet;
	}

}
