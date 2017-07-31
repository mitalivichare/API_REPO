package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.stackroute.datamunger.parser.Criteria;
import com.stackroute.datamunger.parser.QueryParameter;
import com.stackroute.datamunger.processor.Query;

public class SimpleQuery implements Query{
	
	private DataReader dataReader=new DataReader();
	Map<Integer, ArrayList<String>> dataSet;
	private BufferedReader bufferedReader=null;
	private String line = "";
	ArrayList<String> rowData; //arraylist to store records
	ArrayList<Boolean> flags=null;//arraylist to store result of evaluation of the individual criteria
	ArrayList<String> requiredColumnList;
	ArrayList<Criteria> criteriaList;
	ArrayList<String> logicalOperatorList;

	@Override
	public Map<Integer, ArrayList<String>> executeQuery(QueryParameter queryParser)
	{
		String tableName=queryParser.getTableName();
		requiredColumnList=queryParser.getRequiredColumnList();
		String[] header=dataReader.getAllHeaders(tableName);
		criteriaList=queryParser.getCriteriaList();
		logicalOperatorList=queryParser.getLogicalOperatorList();
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
