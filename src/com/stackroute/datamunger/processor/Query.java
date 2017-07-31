package com.stackroute.datamunger.processor;

import java.util.ArrayList;
import java.util.Map;

import com.stackroute.datamunger.parser.QueryParameter;

public interface Query 
{
	public Map<Integer,ArrayList<String>> executeQuery(QueryParameter queryParser);

}
