/* MY ADDITION */

package org.apache.jena.sdb.layout1;

import org.apache.jena.sdb.sql.SDBConnection;
import org.apache.jena.sdb.store.TableDesc;
import static org.apache.jena.sdb.util.StrUtils.sqlList ;

public class TupleLoaderSimpleHive extends TupleLoaderSimple {

	private int counter = 0;
	private int counterThreshold = 1000;
	private int lengthThreshold = 10000000;
	private String insertQuery = "INSERT INTO Triples VALUES\n ";
	private String temp;
	
	TupleLoaderSimpleHive(SDBConnection connection, TableDesc tableDesc, EncoderDecoder codec){
		super(connection, tableDesc, codec);
	}
	
	@Override
	protected void loadRow(String[] vals){
				
		temp = "( " + sqlList(vals) + " )";
		if(counter == 0){
			insertQuery += temp;
		}
		else{
			if(insertQuery.length() + temp.length() > lengthThreshold){
				this.executeInsertQuery();
				this.reset();
				insertQuery += temp;
			}
			else
				insertQuery += ", " +  temp;
		}
		counter ++;
		if(counter == counterThreshold || insertQuery.length() > lengthThreshold ){
			this.executeInsertQuery();
			this.reset();
		}
	}
	
	public void executeInsertQuery(){
		exec(insertQuery);
	}
	
	@Override
	public void finish()
    {
        /* write the remaining rows */
		if(counter > 0){
        	this.executeInsertQuery();
        }
		super.finish();
    }
	
	private void reset(){
		counter = 0;
		insertQuery = "INSERT INTO Triples VALUES\n ";
	}
}
