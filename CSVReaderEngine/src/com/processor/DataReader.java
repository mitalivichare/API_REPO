package com.processor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class DataReader 
{
	//creating object
	private String[] fileHeaders;
	private BufferedReader bufferedReader;
	private String line = "";
	ArrayList<String> rowData; //arraylist to store records
	String[] lineDataArray;
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
		try {
			bufferedReader = new BufferedReader(new FileReader("d:\\"+ queryParser.geTableName().trim() +".csv"));
			bufferedReader.readLine();
			while ((line = bufferedReader.readLine()) != null) {
				String[] lineDataArray=line.split(",");
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
	public ArrayList<Integer> getAllColumnIndexes(ArrayList<String> requiredColumnList, String[] headerColummnArray) {
		ArrayList<Integer> indexes=new ArrayList<>();
		int requiredColumnListLength=requiredColumnList.size();
		int headerColummnArrayLength= headerColummnArray.length;
		for (int i=0; i<requiredColumnListLength; i++) {
			for (int j=0; j<headerColummnArrayLength; j++) {
				if (requiredColumnList.get(i).trim().equalsIgnoreCase(headerColummnArray[j].trim()))
					indexes.add(j);
			}
		}
		return indexes;
	}//end of getAllColumnIndex method

	// Method works on getting indexes of columns in where clause 
	public ArrayList<Integer> getCriteriaListColumnIndexes(ArrayList<Criteria> criteriaList, String[] headerColumnArray) {
		ArrayList<Integer> criteriaListColumnIndexes=new ArrayList<>();
		int headerColumnArrayLength=headerColumnArray.length;
		for (Criteria criteria:criteriaList) {
			for (int i=0; i<headerColumnArrayLength; i++) {
				if (criteria.getColumnName().trim().equalsIgnoreCase(headerColumnArray[i]))
					criteriaListColumnIndexes.add(i);
			}

		}
		return criteriaListColumnIndexes;
	}//end of getCriteriaListColumnIndex method
	
	public Map<Integer, ArrayList<String>> fetchSelectiveColumnsData(ArrayList<String> requiredColumnList, String[] headerColumnArray) {
		Map<Integer, ArrayList<String>> rowMap = new LinkedHashMap<Integer, ArrayList<String>>();
		
		//String[] requiredColumnArray =(String[]) requiredColumnList.toArray();
		int headerColumnArraylength=headerColumnArray.length;
		int requiredColumnArrayLength=requiredColumnList.size();
		int rowId = 0;
		try {
			while ((line=bufferedReader.readLine()) != null) {
				 lineDataArray=line.split(",");
				 rowData=new ArrayList<>();
				for (int i=0; i<requiredColumnArrayLength; i++) {
					for (int j=0; j<headerColumnArraylength; j++) {
						if (requiredColumnList.get(i).trim().equalsIgnoreCase(headerColumnArray[j].trim())) {
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
	
	//method to check the type of comparison operator used
	private boolean checkOperator(String operator, String ClauseValue, String lineData) {
		boolean flag = false;
		switch(operator)
		{
		case ">":
			value=Double.parseDouble(ClauseValue);
			lineValue=Double.parseDouble(lineData);
			if (lineValue>value)
				flag=true;
			break;

		case "<":
			value=Double.parseDouble(ClauseValue);
			lineValue=Double.parseDouble(lineData);
			if (lineValue<value)
				flag=true;
			break;

		case "=":
			String stringValue=ClauseValue;
			String stringLineValue=lineData;
			if (stringLineValue.equals(stringValue)) 
				flag = true;
			break;

		case ">=":
			value=Double.parseDouble(ClauseValue);
			lineValue=Double.parseDouble(lineData);
			if (lineValue>=value)
				flag = true;
			break;

		case "<=":
			value=Double.parseDouble(ClauseValue);
			lineValue=Double.parseDouble(lineData);
			if (lineValue<=value)
				flag=true;
			break;

		case "!=":
			String newValue=ClauseValue;
			String newLineValue=lineData;
			if (!(newLineValue.equals(newValue)))
				flag=true;
			break;
		}
		return flag;
	}
	
	//method to 
	public Map<Integer, ArrayList<String>> fetchDataWithwhereClause(ArrayList<String> requiredColumList, String[] headersArray,
			ArrayList<Criteria> criteriaList, ArrayList<String> logicalOperatorList) {
		Map<Integer, ArrayList<String>> rowMap = new LinkedHashMap<Integer, ArrayList<String>>();
		int rowCount = 0;
		ArrayList<Integer> indexes = getAllColumnIndexes(requiredColumList, headersArray);
		ArrayList<Integer> whereIndexes = getCriteriaListColumnIndexes(criteriaList, headersArray);

		try {
			while ((line = bufferedReader.readLine()) != null) {
				String[] lineData = line.split(",");
				ArrayList<String> rowData=new ArrayList<>();
				ArrayList<Boolean> flags=new ArrayList<>();

				boolean flag = false;

				if (logicalOperatorList.size() > 0) {
					for (int counter=0; counter<=logicalOperatorList.size(); counter++) {
						if (criteriaList.get(counter).getColumnName().equalsIgnoreCase(headersArray[whereIndexes.get(counter)])) {
							flags.add(checkOperator(criteriaList.get(counter).getOperator(),
									criteriaList.get(counter).getValue(), lineData[whereIndexes.get(counter)]));
						}
					}
					for (int counter=0; counter<logicalOperatorList.size(); counter++) {
						if (logicalOperatorList.get(counter).equalsIgnoreCase("and")) {
							flag =flags.get(counter) && flags.get(counter + 1);
						} else if (logicalOperatorList.get(counter).equalsIgnoreCase("or")) {
							flag =flags.get(counter) || flags.get(counter + 1);
						}
					}

				} else {
					if (criteriaList.get(0).getColumnName().trim().equalsIgnoreCase(headersArray[whereIndexes.get(0)].trim())) {
						flag =checkOperator(criteriaList.get(0).getOperator(), criteriaList.get(0).getValue(),
								lineData[whereIndexes.get(0)]);
					}
				}
				if (flag) {
					for (Integer i : indexes) {
						rowData.add(lineData[i]);
					}

					if (flag) {
						rowMap.put(rowCount, rowData);
						rowCount++;
					}
				}
			}
		} catch (IOException e) 
		{}
		return rowMap;
	}
}
