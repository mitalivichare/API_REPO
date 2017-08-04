package com.stackroute.datamunger.processor;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Map;

public class GroupByQueryProcessor implements Query{
	
	private Map<Integer,ArrayList<String>> dataSet;
	private QueryUtility dataReader=new QueryUtility();
	private String[] header;
	private BufferedReader bufferedReader=null;
	private ArrayList<String> requireColumnList;
	private ArrayList<String> rowData;

	@Override
	public Map<Integer, ArrayList<String>> executeQuery(QueryParameter queryParser) 
	{
		// TODO Auto-generated method stub
		
		return null;
	}

}
