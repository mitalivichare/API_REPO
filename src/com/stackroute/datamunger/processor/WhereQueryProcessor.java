package com.stackroute.datamunger.processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.stackroute.datamunger.data.Criteria;

public class WhereQueryProcessor implements Query
{
	
	private DataReader dataReader=new DataReader();
	Map<Integer, ArrayList<String>> dataSet;
	private BufferedReader bufferedReader=null;
	private String row = "";
	ArrayList<String> rowData; //arraylist to store records
	ArrayList<Boolean> flags=null;//arraylist to store result of evaluation of the individual criteria
	ArrayList<String> requiredColumnList;
	ArrayList<Criteria> criteriaList;
	ArrayList<String> logicalOperatorList;
	String[] header;
	String fileName;

	@Override
	public Map<Integer, ArrayList<String>> executeQuery(QueryParameter queryParser) {
		// TODO Auto-generated method stub
		
		fileName=queryParser.getFileName();
		requiredColumnList=queryParser.getRequiredColumnList();
		header=dataReader.getAllHeaders(fileName);
		criteriaList=queryParser.getCriteriaList();
		logicalOperatorList=queryParser.getLogicalOperatorList();
		bufferedReader=dataReader.getBufferedReader();
		dataSet=new LinkedHashMap<Integer, ArrayList<String>>();
		int rowId = 0;
		String[] rowsArray=null;
		ArrayList<Integer> requiredColumnIndexes = dataReader.getAllColumnIndexes(requiredColumnList, header);
		ArrayList<Integer> whereColumnIndexes = dataReader.getCriteriaListColumnIndexes(criteriaList, header);

		try
		{
			while ((row = bufferedReader.readLine()) != null)
			{
				rowsArray= row.split(",");
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
							flags.add(dataReader.evaluateCriteria(criteriaList.get(counter).getOperator(),criteriaList.get(counter).getValue(), rowsArray[whereColumnIndexes.get(counter)]));
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
						flag =dataReader.evaluateCriteria(criteriaList.get(0).getOperator(), criteriaList.get(0).getValue(),rowsArray[whereColumnIndexes.get(0)]);
					}
				}
				if (flag)
				{
					for (Integer columnIndex : requiredColumnIndexes)
					{
						rowData.add(rowsArray[columnIndex]);
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

}
