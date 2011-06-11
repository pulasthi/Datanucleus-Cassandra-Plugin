

package com.spidertracks.datanucleus.query;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/** 
 * class to evaluate a given data object and figure out the properties of the 
 * given class such as indexed and nonindexed fields
 * 
 *  @author pulasthi supun
 */

public class AnnotaionEvaluator {
	
	private Map<String,ArrayList<String>> annotaionMap = new HashMap<String, ArrayList<String>> ();
	private Class<? extends Object> cls;

	public AnnotaionEvaluator(){
		
		
	}
	public AnnotaionEvaluator(Class<? extends Object> cls){
		this.cls = cls;
		this.annotaionMap = getFieldAnnotaionMap(cls);
	}
	

	/**
	 * calls 	getFieldAnnotaionMap(Class)
	 * @param dataObject
	 * @return the Map containing each type and there respective fields array
	 */
	public Map<String,ArrayList<String>> getFieldAnnotaionMap(Object dataObject){
		cls = dataObject.getClass();
		
		return getFieldAnnotaionMap(cls);
	}
	/**
	 * loops through the class fields and finds Annotaions defined for each field and groups 
	 * together the fields with the same Annotaion
	 * @param cls
	 * @return the Map containing each type and there respective fields array
	 */
	public Map<String,ArrayList<String>> getFieldAnnotaionMap(Class<? extends Object> cls){
		this.cls = cls;
		for(Field field : cls.getDeclaredFields()){

			String name = field.getName();
			Annotation[] annotations = field.getDeclaredAnnotations();
			for(Annotation ann : annotations){
				String type = ann.annotationType().getName();
				
				if(annotaionMap.containsKey(type)){
					annotaionMap.get(type).add(name);
				}else{
					annotaionMap.put(type, new ArrayList<String>());
					annotaionMap.get(type).add(name);
				}
			}
			
		}	 
		
		return annotaionMap;
	}
	/**
	 * Retrieves the list of fields with the given Annotaion 
	 * @param annotaion
	 * @return a ArrayList which contains all the fields that have the given annotaion
	 */
	public ArrayList<String> getAnnotatedFields(String annotaion){
		ArrayList<String> annotatedFields = null;
		if(annotaion == null){
			return null;
		}
		if(annotaion.startsWith("@")){
			String type = annotaion.substring(1);
			String annotaiontype = "javax.jdo.annotations." + type;
			annotatedFields = annotaionMap.get(annotaiontype);
		}else{
			annotatedFields = annotaionMap.get(annotaion);
		}
		
		
		return annotatedFields;
	}
	
}
