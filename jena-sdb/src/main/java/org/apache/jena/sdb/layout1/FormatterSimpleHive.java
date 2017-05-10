/* MY ADDITION */

package org.apache.jena.sdb.layout1;

import static org.apache.jena.sdb.sql.SQLUtils.sqlStr ;

import java.sql.SQLException;

import org.apache.jena.sdb.SDBException ;
import org.apache.jena.sdb.layout2.TablePrefixes ;
import org.apache.jena.sdb.sql.SDBConnection ;
import org.apache.jena.sdb.sql.TableUtils ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FormatterSimpleHive extends FormatterSimple 
{
    //private static Logger log = LoggerFactory.getLogger(FormatterSimpleHive.class) ;
    
    //private static final String colDecl = "VARCHAR("+UriWidth+")" ;
	private static final String colDecl = "String" ;
    public FormatterSimpleHive(SDBConnection connection)
    { 
        super(connection) ;
    }
    
    @Override
    public void truncate()
    {
        try { 
            connection().exec("TRUNCATE TABLE Triples") ;
        } catch (SQLException ex)
        {
            //log.warn("Exception truncating tables") ;
            throw new SDBException("SQLException truncating tables",ex) ;
        }
    }
    
    @Override
    public void format()
    {
        reformatPrefixesWorker(false) ;
        reformatDataWorker() ;
    }
    
    private void reformatPrefixesWorker() { reformatPrefixesWorker(false) ; }
    private void reformatPrefixesWorker(boolean loadPrefixes)
    {
        try { 
            dropTable("Prefixes") ;
            connection().exec(sqlStr(
                    "CREATE TABLE Prefixes (prefix VARCHAR("+TablePrefixes.prefixColWidth+"), uri VARCHAR("+TablePrefixes.uriColWidth+"))"));
            if ( loadPrefixes )
            {
                connection().execUpdate("INSERT INTO Prefixes VALUES ('x',       'http://example/')") ;
                connection().execUpdate("INSERT INTO Prefixes VALUES ('ex',      'http://example.org/')") ;
                connection().execUpdate("INSERT INTO Prefixes VALUES ('rdf',     'http://www.w3.org/1999/02/22-rdf-syntax-ns#')") ;
                connection().execUpdate("INSERT INTO Prefixes VALUES ('rdfs',    'http://www.w3.org/2000/01/rdf-schema#')") ;
                connection().execUpdate("INSERT INTO Prefixes VALUES ('xsd',     'http://www.w3.org/2001/XMLSchema#')") ;
                connection().execUpdate("INSERT INTO Prefixes VALUES ('owl' ,    'http://www.w3.org/2002/07/owl#')") ;
                connection().execUpdate("INSERT INTO Prefixes VALUES ('foaf',    'http://xmlns.com/foaf/0.1/')") ;
                connection().execUpdate("INSERT INTO Prefixes VALUES ('dc',      'http://purl.org/dc/elements/1.1/')") ;
                connection().execUpdate("INSERT INTO Prefixes VALUES ('dcterms', 'http://purl.org/dc/terms/')") ;
            }
            
        } catch (SQLException ex)
        {
            //log.warn("Exception resetting table 'Prefixes'") ; 
            throw new SDBException("SQLException resetting table 'Prefixes'",ex) ;
        }
    }
    
    private void reformatDataWorker()
    {
        
        try {
            dropTable("Triples") ;
            connection().exec(sqlStr("CREATE TABLE Triples(s "+colDecl+", p "+colDecl+", o "+colDecl+" ) "
            					   + "STORED AS ORC tblproperties (\"orc.compress\" = \"SNAPPY\")"));
            					   
        } catch (SQLException ex)
        {
            throw new SDBException("SQLException resetting table 'Triples'",ex) ;
        }
    }
    
    @Override
    /* Indexes unsupported for Tez execution engine */
    public void addIndexes()
    {
//        try {
//            connection().exec("CREATE INDEX PredObj ON TABLE "+TableDescSPO.name()+" (p, o) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD ") ;
//            connection().exec("CREATE INDEX ObjSubj ON TABLE "+TableDescSPO.name()+" (o, s) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD ") ;
//        } catch (SQLException ex)
//        {
//            throw new SDBException("SQLException indexing table 'Triples'",ex) ;
//        }
    }
    
    protected void dropTable(String tableName)
    {
        TableUtils.dropTable(connection(), tableName) ;
    }
}
