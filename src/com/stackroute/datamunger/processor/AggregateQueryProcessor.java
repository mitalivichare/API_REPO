package com.stackroute.datamunger.processor;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.stackroute.datamunger.data.AggregateFunctions;

public class AggregateQueryProcessor implements Query 
{
	private QueryUtility queryUtility = new QueryUtility();
	private BufferedReader bufferedReader = null;
	private String row = null;
	private String fileName = null;
	private String[] rowsArray = null;
	private ArrayList<String> header = null;
	private Map<Integer, ArrayList<String>> dataSet = null;
	private ArrayList<String> rowData = null;
	private ArrayList<AggregateFunctions> aggregateFunctionList = null;
	private ArrayList<Integer> aggregateColumnIndexes = new ArrayList<>();
	double count = 0.0;
	double value1 = 0.0;
	double value2 = 0.0;
	double result = 0.0;
	int rowCount=0;

	@Override
	public Map<Integer, ArrayList<String>> executeQuery(QueryParameter queryParameter) 
	{
		// TODO Auto-generated method stub
		fileName = queryParameter.getFileName();
		header = queryUtility.getAllHeaders(fileName);
		aggregateFunctionList = queryParameter.getAggregateFunctionsList();
		dataSet = new LinkedHashMap<>();
		int rowIndex = 0;

		for (AggregateFunctions listObject : aggregateFunctionList)
		{
			aggregateColumnIndexes.add(queryUtility.getSpecificColumnIndex(listObject.getColumnName(), header));
		}

		try
		{
			bufferedReader = queryUtility.getBufferedReader();
			while ((this.row = bufferedReader.readLine()) != null)
			{
				
				rowsArray = row.split("(\\s*,\\s*)");
				int counter = 0;
				for (AggregateFunctions listObject : aggregateFunctionList)
				{
					//String columnValue=lineData[aggregateColumnIndexes.get(counter)];
					evaluateAggregateFunction(listObject, rowsArray[aggregateColumnIndexes.get(counter)]);
					counter++;
				}
			}
		}
		catch (Exception e)
		{}

		for (AggregateFunctions listObject : aggregateFunctionList)
		{
			rowData = new ArrayList<>();
			rowData.add(listObject.getFunctionName());
			rowData.add(listObject.getColumnName());
			if(listObject.getFunctionName().equalsIgnoreCase("avg"))
			{
				double avgResult=(listObject.getResult())/rowCount;
				rowData.add(Double.toString(avgResult));
			}
			else
			{
			rowData.add(Double.toString(listObject.getResult()));
			}
			dataSet.put(rowIndex, rowData);
			rowIndex++;
		}
		return dataSet;
	}
	//Method to evaluate aggregate function and set the result accordingly
	private void evaluateAggregateFunction(AggregateFunctions aggregateFunction, String columnValue)
	{
		if(!(columnValue.isEmpty()))
		{
		 rowCount++;
		switch (aggregateFunction.getFunctionName())
		{
			case "sum":
				
				try
				{
					value1 = Double.parseDouble(columnValue);
					value2 = aggregateFunction.getResult();
					result = value1 + value2;
					aggregateFunction.setResult(result);
					
				}
				catch (Exception e) 
				{}
				break;
	
			case "count":
				if(columnValue!=null)
				{
					aggregateFunction.setResult(count++);
				}
				break;
	
			case "min":
				
				value1 = Double.parseDouble(columnValue);
				value2 = aggregateFunction.getResult();
				if (value2 == 0.0)
				{
					result = value1;
					aggregateFunction.setResult(result);
					
				}
				else if (value1 < value2)
				{
					result = value1;
					aggregateFunction.setResult(result);
					
				}
				break;
	
			case "max":
				
				value1 = Double.parseDouble(columnValue);
				value2 = aggregateFunction.getResult();
	
				if (value2 == 0.0)
				{
					result = value1;
					aggregateFunction.setResult(result);
				}
				else if (value1 > value2)
				{
					result = value1;
					aggregateFunction.setResult(result);
				}
				break;
	
			case "avg":
				
				try
				{
					value1 = Double.parseDouble(columnValue);
					value2 = aggregateFunction.getResult();
					result = value1 + value2;
					aggregateFunction.setResult(result);
				} catch (Exception e) 
				{}
				
				break;
	
			default:
				break;
		}
				}
	}
}
