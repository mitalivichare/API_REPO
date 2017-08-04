package com.stackroute.datamunger.processor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import com.stackroute.datamunger.data.Criteria;

public class QueryUtility 
{
	String[] headers;
	private ArrayList<String> fileHeaders;
	private BufferedReader bufferedReader;
	private String row = "";
	ArrayList<String> rowData; //arraylist to store records
	
	ArrayList<Boolean> flags=null;//arraylist to store result of evaluation of the individual criteria
	private double value,lineValue;
	
	//method for reading firstline of the file i.e the header columns
	public ArrayList<String> getAllHeaders(String fileName) 
	{
		try 
		{
			bufferedReader = new BufferedReader(new FileReader(fileName));
			row = bufferedReader.readLine();
			fileHeaders =new ArrayList<String> (Arrays.asList(row.split(",")));
		
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return fileHeaders; //returning the arrayList holding header columns
	}//end of fetchHeader method
	
	//Method works on getting indexes of all columns in the file
	public ArrayList<Integer> getAllColumnIndexes(ArrayList<String> requiredColumnList, ArrayList<String> headerColumnArray) {
		ArrayList<Integer> indexes=new ArrayList<>();
		int requiredColumnListLength=requiredColumnList.size();
		int headerColummnArrayLength= headerColumnArray.size();
		for (int i=0; i<requiredColumnListLength; i++)
		{
			for (int j=0; j<headerColummnArrayLength; j++)
			{
				if (requiredColumnList.get(i).trim().equalsIgnoreCase(headerColumnArray.get(j).trim()))
					indexes.add(j);
			}
		}
		return indexes;//returns the index of all columns
	}//end of getAllColumnIndex method
	
	public int getSpecificColumnIndex(String selectedColumn, ArrayList<String> headers) {
		int headerLength = headers.size();
		int index = -1;
		for (int j = 0; j < headerLength; j++) {
			if (selectedColumn.trim().equalsIgnoreCase(headers.get(j).trim())) {
				index = Integer.valueOf(j);
			}
		}
		return index;
	}

	// Method works on getting indexes of columns in where clause 
	public ArrayList<Integer> getCriteriaListColumnIndexes(ArrayList<Criteria> criteriaList, ArrayList<String> headerColumnArray) {
		ArrayList<Integer> criteriaListColumnIndexes=new ArrayList<>();
		int headerColumnArrayLength=headerColumnArray.size();
		for (Criteria criteria:criteriaList) {
			for (int i=0; i<headerColumnArrayLength; i++)
			{
				if (criteria.getColumnName().trim().equalsIgnoreCase(headerColumnArray.get(i)))
				{
					criteriaListColumnIndexes.add(i);
				}
			}

		}
		return criteriaListColumnIndexes;
	}//end of getCriteriaListColumnIndex method
	
	//method works to check the type of comparison operator used in the criteriaList
	public boolean evaluateCriteria(String operator, String criteriaValue, String actualLineValue)
	{
		boolean flag = false;
		if(!(actualLineValue.isEmpty()))
		{
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
		}
		return flag;
	}

	public BufferedReader getBufferedReader() {
		return bufferedReader;
	}

	public void setBufferedReader(BufferedReader bufferedReader) {
		this.bufferedReader = bufferedReader;
	}
	
}

