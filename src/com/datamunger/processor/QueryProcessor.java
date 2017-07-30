package com.datamunger.processor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.datamunger.parser.QueryParser;

public class QueryProcessor 
{
	private DataReader dataReader=new DataReader();
	private QueryParser queryParser=new QueryParser();
	Map<Integer,ArrayList<String>> dataSet=new LinkedHashMap<Integer,ArrayList<String>>();
	public Map<Integer,ArrayList<String>> executeQuery(String query)
	{
		queryParser=queryParser.extractParameters(query);
		if(query.contains("*"))
		{
		dataSet=dataReader.fetchAllData(queryParser);
		}
		else if(query.contains("where"))
		{
			dataSet=dataReader.fetchDataWithWhereClause(queryParser.getRequiredColumnList(), dataReader.getAllHeaders(queryParser.geTableName()), queryParser.getCriteriaList(), queryParser.getLogicalOperatorList());
		}
		else
		{
		dataSet=dataReader.fetchSelectiveColumnsData(queryParser.getRequiredColumnList(),dataReader.getAllHeaders(queryParser.geTableName()));
		}
		return dataSet;
	}

}
