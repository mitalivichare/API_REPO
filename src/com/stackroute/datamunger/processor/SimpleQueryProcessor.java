package com.stackroute.datamunger.processor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleQueryProcessor implements Query{
	
	private DataReader dataReader=new DataReader();
	private Map<Integer, ArrayList<String>> dataSet=null;
	private BufferedReader bufferedReader=null;
	private String line = "";
	private ArrayList<String> rowData=null; //arraylist to store records
	private ArrayList<String> requiredColumnList;

	@Override
	public Map<Integer, ArrayList<String>> executeQuery(QueryParameter queryParser)
	{
		String tableName=queryParser.getTableName();
		requiredColumnList=queryParser.getRequiredColumnList();
		String[] header=dataReader.getAllHeaders(tableName);
		bufferedReader=dataReader.getBufferedReader(); 
		
		if(requiredColumnList.contains("*"))
		{
			dataSet = new LinkedHashMap<Integer, ArrayList<String>>();
			int rowId = 0;
			String[] lineDataArray=null;
			try
			{
				bufferedReader = new BufferedReader(new FileReader("d:\\"+ queryParser.geTableName().trim() +".csv"));
				bufferedReader.readLine();
				while ((line = bufferedReader.readLine()) != null)
				{
					lineDataArray=line.split(",");
					rowData=new ArrayList<>();
					for (String data:lineDataArray)
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
			dataSet= new LinkedHashMap<Integer, ArrayList<String>>();
			
			//String[] requiredColumnArray =(String[]) requiredColumnList.toArray();
			int headerColumnArraylength=header.length;
			int requiredColumnArrayLength=requiredColumnList.size();
			int rowId = 0;
			String[] lineDataArray=null;
			try
			{
				while ((line=bufferedReader.readLine()) != null)
				{
					lineDataArray=line.split(",");
					rowData=new ArrayList<>();
					for (int i=0; i<requiredColumnArrayLength; i++)
					{
						for (int j=0; j<headerColumnArraylength; j++)
						{
							if (requiredColumnList.get(i).trim().equalsIgnoreCase(header[j].trim()))
							{
								rowData.add(lineDataArray[j]);
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
		
		// TODO Auto-generated method stub
	}

}
