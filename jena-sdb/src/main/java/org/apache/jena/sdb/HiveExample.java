package org.apache.jena.sdb;

import org.apache.jena.query.*;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sdb.SDBFactory;
import org.apache.jena.sdb.Store;

import com.google.common.base.Stopwatch;

import java.sql.* ;
import java.util.concurrent.TimeUnit;

import gr.di.uoa.jenaspatial.utils.JenaSpatial;

public class HiveExample {
	
	static String data1 = "/home/hduser1/jena/jena-sdb/Datasets/geonames.nt";
	static String data2 = "/home/hduser1/jena/jena-sdb/Datasets/linkedgeodata.nt";
	static String assemblerFile = "/home/hduser1/jena/jena-sdb/sdb-hive.ttl";
	static Store myStore;
	static Model model;
	
	
	public static void main(String[] args) throws SQLException {
		
		//connect to the db
		myStore = SDBFactory.connectStore(assemblerFile);
		//myStore.getConnection().getSqlConnection().setAutoCommit(true);
		//link model and store
		model = SDBFactory.connectDefaultModel(myStore);
		//load data into the model from input file
		formatStoreReadData();
		
		/* load geospatial operations */
		JenaSpatial.load();
		
		//query the data
		queryData();

		
		myStore.getConnection().close();
		myStore.close();
		System.out.println("Finished");
	}
	
	public static void formatStoreReadData(){
		myStore.getTableFormatter().create();
		model.read(data1);
		System.out.println("Loaded GEONAMES");
		model.read(data2);
		System.out.println("Loaded LINKEDGEODATA");	
	}
	
	public static void queryData(){
		
	
		String queryStr   = "PREFIX geof: <http://example.org/function#> \n"
						  + "SELECT ?s1 ?s2\n"
						  + "WHERE { "
						  + "	?s1 <http://linkedgeodata.org/ontology/asWKT> ?o1 . \n"
						  + "   ?s2 <http://geo.linkedopendata.gr/gag/ontology/asWKT> ?o2 . \n"
						  + "   FILTER(geof:sfWithin(?o1, ?o2)) . \n"
						  + "} ";
		
		
		int counter = 0;
		Query query = QueryFactory.create(queryStr);
		Stopwatch timer = new Stopwatch();
		for(int i = 0; i < 3; i++){
			try (QueryExecution qexec = QueryExecutionFactory.create(query, model)){
				timer.start();
				ResultSet results = qexec.execSelect();
			    while(results.hasNext()){
			    	QuerySolution sol = results.next();
			    	System.out.println(sol.toString());
			    	counter ++;
			    }
			    timer.stop();
			    System.out.println((double)timer.elapsed(TimeUnit.MILLISECONDS) / 1000);
			    timer.reset();
			}
		}
		System.out.println(counter);
	}
}
