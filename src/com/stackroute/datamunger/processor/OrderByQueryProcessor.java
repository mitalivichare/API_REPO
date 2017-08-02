package com.stackroute.datamunger.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class OrderByQueryProcessor implements Query {
	
	private Map<Integer,ArrayList<String>> dataSet=null;
	private SimpleQueryProcessor simpleReader=new SimpleQueryProcessor();
	private int orderByColumnIndex;

	@Override
	public Map<Integer, ArrayList<String>> executeQuery(QueryParameter queryParameter) {
		// TODO Auto-generated method stub
		
		dataSet=simpleReader.executeQuery(queryParameter);
		orderByColumnIndex=queryParameter.getOrderByColumnIndex();
		List<Map.Entry<Integer,ArrayList<String>>> entries=new ArrayList<Map.Entry<Integer, ArrayList<String>>>(dataSet.entrySet());
		Collections.sort(entries,new Comparator<Map.Entry<Integer,ArrayList<String>>>()
				{
					@Override
					public int compare(Entry<Integer, ArrayList<String>> data1, Entry<Integer, ArrayList<String>> data2) {
						// TODO Auto-generated method stub
						try
						{
							Double value1=Double.parseDouble(data1.getValue().get(orderByColumnIndex));
							Double value2=Double.parseDouble(data2.getValue().get(orderByColumnIndex));
							return value1.compareTo(value2);
						}
						catch(Exception e)
						{
							return data1.getValue().get(orderByColumnIndex).compareTo(data2.getValue().get(orderByColumnIndex));
						}
					}
				});
		Map<Integer,ArrayList<String>> sortedDataSet=new LinkedHashMap<Integer,ArrayList<String>>();
		for(Map.Entry<Integer, ArrayList<String>> entry : entries)
		{
			sortedDataSet.put(entry.getKey(), entry.getValue());
		}
		return sortedDataSet;
	}
}
