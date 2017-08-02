package com.stackroute.datamunger.processor;

import java.util.ArrayList;
import java.util.Map;

public interface Query 
{
	public Map<Integer,ArrayList<String>> executeQuery(QueryParameter queryParser);

}
