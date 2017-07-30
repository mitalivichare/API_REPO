package com.datamunger.processor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.datamunger.model.Criteria;
import com.datamunger.parser.QueryParser;

public class DataReader 
{
	
	private String[] fileHeaders;
	private BufferedReader bufferedReader;
	private String line = "";
	ArrayList<String> rowData; //arraylist to store records
	
	ArrayList<Boolean> flags=null;//arraylist to store result of evaluation of the individual criteria
	private double value,lineValue;
	
	//method for reading firstline of the file i.e the header columns
	public String[] getAllHeaders(String tableName) 
	{
		try 
		{
			bufferedReader = new BufferedReader(new FileReader("d:\\"+ tableName +".csv"));
			line = bufferedReader.readLine();
			fileHeaders = line.split(",");
		}
		catch (IOException e) {
		}
		return fileHeaders; //returning the array holding header columns
	}//end of fetchHeader method
	
	//method to fetch all the data from csv file
	public Map<Integer, ArrayList<String>> fetchAllData(QueryParser queryParser) 
	{
		Map<Integer, ArrayList<String>> rowMap = new LinkedHashMap<Integer, ArrayList<String>>();
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
				for (String string:lineDataArray)
					rowData.add(string);
				rowMap.put(rowId,rowData);
				rowId++;
			}
		} catch (IOException e)
		{}
		return rowMap; //returning map having row_id and arrayList containing complete file  

	}//end of fetchAllData method
	
	//Method works on getting indexes of all columns in the file
	private ArrayList<Integer> getAllColumnIndexes(ArrayList<String> requiredColumnList, String[] headerColumnArray) {
		ArrayList<Integer> indexes=new ArrayList<>();
		int requiredColumnListLength=requiredColumnList.size();
		int headerColummnArrayLength= headerColumnArray.length;
		for (int i=0; i<requiredColumnListLength; i++)
		{
			for (int j=0; j<headerColummnArrayLength; j++)
			{
				if (requiredColumnList.get(i).trim().equalsIgnoreCase(headerColumnArray[j].trim()))
					indexes.add(j);
			}
		}
		return indexes;//returns the index of all columns
	}//end of getAllColumnIndex method

	// Method works on getting indexes of columns in where clause 
	private ArrayList<Integer> getCriteriaListColumnIndexes(ArrayList<Criteria> criteriaList, String[] headerColumnArray) {
		ArrayList<Integer> criteriaListColumnIndexes=new ArrayList<>();
		int headerColumnArrayLength=headerColumnArray.length;
		for (Criteria criteria:criteriaList) {
			for (int i=0; i<headerColumnArrayLength; i++)
			{
				if (criteria.getColumnName().trim().equalsIgnoreCase(headerColumnArray[i]))
				{
					criteriaListColumnIndexes.add(i);
				}
			}

		}
		return criteriaListColumnIndexes;
	}//end of getCriteriaListColumnIndex method
	
	public Map<Integer, ArrayList<String>> fetchSelectiveColumnsData(ArrayList<String> requiredColumnList, String[] headerColumnArray)
	{
		Map<Integer, ArrayList<String>> rowMap = new LinkedHashMap<Integer, ArrayList<String>>();
		
		//String[] requiredColumnArray =(String[]) requiredColumnList.toArray();
		int headerColumnArraylength=headerColumnArray.length;
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
						if (requiredColumnList.get(i).trim().equalsIgnoreCase(headerColumnArray[j].trim()))
						{
							rowData.add(lineDataArray[j]);
						}
					}
				}
				rowMap.put(rowId,rowData);
				rowId++;
			}
		} catch (IOException e) 
		{}
		return rowMap;
	}
	
	//method works to check the type of comparison operator used in the criteriaList
	private boolean evaluateCriteria(String operator, String criteriaValue, String actualLineValue)
	{
		boolean flag = false;
		switch(operator)
		{
		case ">":
			value=Double.parseDouble(criteriaValue);
			lineValue=Double.parseDouble(actualLineValue);
			if (lineValue>value)
				flag=true;
			break;

		case "<":
			value=Double.parseDouble(criteriaValue);
			lineValue=Double.parseDouble(actualLineValue);
			if (lineValue<value)
				flag=true;
			break;

		case "=":
			String stringValueForEqualCase=criteriaValue;
			String stringLineValueForEqualCase=actualLineValue;
			if (stringLineValueForEqualCase.equals(stringValueForEqualCase)) 
				flag = true;
			break;

		case ">=":
			value=Double.parseDouble(criteriaValue);
			lineValue=Double.parseDouble(actualLineValue);
			if (lineValue>=value)
				flag = true;
			break;

		case "<=":
			value=Double.parseDouble(criteriaValue);
			lineValue=Double.parseDouble(actualLineValue);
			if (lineValue<=value)
				flag=true;
			break;

		case "!=":
			String stringValueForNotEqualCase=criteriaValue;
			String stringLineValueForNotEqualCase=actualLineValue;
			if (!(stringLineValueForNotEqualCase.equals(stringValueForNotEqualCase)))
				flag=true;
			break;
		}
		return flag;
	}
	
	//method to fetch data using where condition, both single or multiple
	public Map<Integer, ArrayList<String>> fetchDataWithWhereClause(ArrayList<String> requiredColumList, String[] headerArray,
			ArrayList<Criteria> criteriaList, ArrayList<String> logicalOperatorList)
	{
		Map<Integer, ArrayList<String>> rowMap = new LinkedHashMap<Integer, ArrayList<String>>();
		int rowId = 0;
		String[] lineData=null;
		ArrayList<Integer> requiredColumnIndexes = getAllColumnIndexes(requiredColumList, headerArray);
		ArrayList<Integer> whereColumnIndexes = getCriteriaListColumnIndexes(criteriaList, headerArray);

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
						if (criteriaList.get(counter).getColumnName().equalsIgnoreCase(headerArray[whereColumnIndexes.get(counter)]))
						{
							flags.add(evaluateCriteria(criteriaList.get(counter).getOperator(),criteriaList.get(counter).getValue(), lineData[whereColumnIndexes.get(counter)]));
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
					if (criteriaList.get(0).getColumnName().trim().equalsIgnoreCase(headerArray[whereColumnIndexes.get(0)].trim()))
					{
						flag =evaluateCriteria(criteriaList.get(0).getOperator(), criteriaList.get(0).getValue(),lineData[whereColumnIndexes.get(0)]);
					}
				}
				if (flag)
				{
					for (Integer columnIndex : requiredColumnIndexes)
					{
						rowData.add(lineData[columnIndex]);
					}
					rowMap.put(rowId, rowData);
					rowId++;
				}
			}
		}
		catch (IOException e) 
		{}
		return rowMap;
	}
}
