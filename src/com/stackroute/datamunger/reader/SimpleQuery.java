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
	private BufferedReader bufferedReader;
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
		
		if(criteriaList != null)
		{
			dataSet=new LinkedHashMap<Integer, ArrayList<String>>();
			int rowId = 0;
			String[] lineData=null;
			ArrayList<Integer> requiredColumnIndexes = dataReader.getAllColumnIndexes(requiredColumnList, header);
			ArrayList<Integer> whereColumnIndexes = dataReader.getCriteriaListColumnIndexes(criteriaList, header);

			try
			{
				while ((line = bufferedReader.readLine()) != null)
				{
					lineData= line.split(",");
					rowData=new ArrayList<>();
					flags=new ArrayList<>();

					boolean flag = false;
					//loop to evaluate criteria list
					if (logicalOperatorList.size() > 0)
					{
						for (int counter=0; counter<=logicalOperatorList.size(); counter++)
						{
							if (criteriaList.get(counter).getColumnName().equalsIgnoreCase(header[whereColumnIndexes.get(counter)]))
							{
								flags.add(dataReader.evaluateCriteria(criteriaList.get(counter).getOperator(),criteriaList.get(counter).getValue(), lineData[whereColumnIndexes.get(counter)]));
							}
						}
						for (int counter=0; counter<logicalOperatorList.size(); counter++)
						{
							if (logicalOperatorList.get(counter).equalsIgnoreCase("and"))
							{
								flag =flags.get(counter) && flags.get(counter + 1);
							}
							else if (logicalOperatorList.get(counter).equalsIgnoreCase("or"))
							{
								flag =flags.get(counter) || flags.get(counter + 1);
							}
						}
					}
					else
					{
						if (criteriaList.get(0).getColumnName().trim().equalsIgnoreCase(header[whereColumnIndexes.get(0)].trim()))
						{
							flag =dataReader.evaluateCriteria(criteriaList.get(0).getOperator(), criteriaList.get(0).getValue(),lineData[whereColumnIndexes.get(0)]);
						}
					}
					if (flag)
					{
						for (Integer columnIndex : requiredColumnIndexes)
						{
							rowData.add(lineData[columnIndex]);
						}
						dataSet.put(rowId, rowData);
						rowId++;
					}
				}
			}
			catch (IOException e) 
			{}
			return dataSet;
		}
		else if(requiredColumnList.contains("*"))
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
