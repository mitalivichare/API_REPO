package com.stackroute.datamunger.processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.stackroute.datamunger.data.Criteria;

public class WhereQueryProcessor implements Query
{
	
	private QueryUtility queryUtility=new QueryUtility();
	private Map<Integer, ArrayList<String>> dataSet;
	private BufferedReader bufferedReader=null;
	private String row = "";
	private ArrayList<String> rowData; //arraylist to store records
	private ArrayList<Boolean> flags=null;//arraylist to store result of evaluation of the individual criteria
	private ArrayList<String> requiredColumnList;
	private ArrayList<Criteria> criteriaList;
	private ArrayList<String> logicalOperatorList;
	private ArrayList<String> header;
	private String fileName;

	@Override
	public Map<Integer, ArrayList<String>> executeQuery(QueryParameter queryParser) {
		// TODO Auto-generated method stub
		
		fileName=queryParser.getFileName();
		requiredColumnList=queryParser.getRequiredColumnList();
		header=queryUtility.getAllHeaders(fileName);
		criteriaList=queryParser.getCriteriaList();
		logicalOperatorList=queryParser.getLogicalOperatorList();
		bufferedReader=queryUtility.getBufferedReader();
		dataSet=new LinkedHashMap<Integer, ArrayList<String>>();
		int rowId = 0;
		String[] rowsArray=null;
		ArrayList<Integer> requiredColumnIndexes = queryUtility.getAllColumnIndexes(requiredColumnList, header);
		ArrayList<Integer> whereColumnIndexes = queryUtility.getCriteriaListColumnIndexes(criteriaList, header);

		try
		{
			while ((row = bufferedReader.readLine()) != null)
			{
				rowsArray= row.split("(\\s*,\\s*)");
				rowData=new ArrayList<>();
				flags=new ArrayList<>();

				boolean flag = false;
				//loop to evaluate criteria list
				if (logicalOperatorList.size() > 0)
				{
					for (int counter=0; counter<=logicalOperatorList.size(); counter++)
					{
						if (criteriaList.get(counter).getColumnName().equalsIgnoreCase(header.get(whereColumnIndexes.get(counter))))
						{
							flags.add(queryUtility.evaluateCriteria(criteriaList.get(counter).getOperator(),criteriaList.get(counter).getValue(), rowsArray[whereColumnIndexes.get(counter)]));
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
					if (criteriaList.get(0).getColumnName().trim().equalsIgnoreCase(header.get(whereColumnIndexes.get(0)).trim()))
					{
						flag =queryUtility.evaluateCriteria(criteriaList.get(0).getOperator(), criteriaList.get(0).getValue(),rowsArray[whereColumnIndexes.get(0)]);
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
