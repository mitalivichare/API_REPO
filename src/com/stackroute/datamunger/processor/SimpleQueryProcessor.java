package com.stackroute.datamunger.processor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleQueryProcessor implements Query{
	
	private QueryUtility queryUtility=new QueryUtility();
	private Map<Integer, ArrayList<String>> dataSet=null;
	private BufferedReader bufferedReader=null;
	private String row = "";
	private ArrayList<String> rowData=null; //arraylist to store records
	private ArrayList<String> requiredColumnList;
	private ArrayList<String> header;
	private int headerColumnArraylength, requiredColumnArrayLength,rowId;
	private String[] rowsArray;
	int indexes=0;


	@Override
	public Map<Integer, ArrayList<String>> executeQuery(QueryParameter queryParser)
	{
		String tableName=queryParser.getFileName();
		requiredColumnList=queryParser.getRequiredColumnList();
		header=queryUtility.getAllHeaders(tableName);
		bufferedReader=queryUtility.getBufferedReader(); 
		dataSet = new LinkedHashMap<Integer, ArrayList<String>>();
		headerColumnArraylength=header.size();
		requiredColumnArrayLength=requiredColumnList.size();
		rowsArray=null;
		rowId = 0;
		
		if(requiredColumnList.contains("*"))
		{
			try
			{
				bufferedReader = new BufferedReader(new FileReader(queryParser.getFileName().trim()));
				bufferedReader.readLine();
				while ((row = bufferedReader.readLine()) != null)
				{
					rowsArray=row.split("(\\s*,\\s*)");
					rowData=new ArrayList<>();
					for (String data:rowsArray)
						rowData.add(data);
					dataSet.put(rowId,rowData);
					rowId++;
				}
			} catch (IOException e)
			{}
			return dataSet;
			 //returning map having row_id and arrayList containing complete file
		}
		else
		{
			try
			{
				while ((row=bufferedReader.readLine()) != null)
				{
					rowsArray=row.split("(\\s*,\\s*)");
					rowData=new ArrayList<>();
					
					for (int i=0; i<requiredColumnArrayLength; i++)
					{
						for (int j=0; j<headerColumnArraylength; j++)
						{
							if (requiredColumnList.get(i).trim().equalsIgnoreCase(header.get(j).trim()))
							{
								rowData.add(rowsArray[j]);
							}
						}
					}
					dataSet.put(rowId,rowData);
					rowId++;
				}
			} catch (IOException e) 
			{}
			return dataSet;
		}
	}
}
