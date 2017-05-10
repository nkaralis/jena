/* MY ADDITION */

package org.apache.jena.sdb.layout1;

import org.apache.jena.sdb.StoreDesc ;
import org.apache.jena.sdb.core.sqlnode.GenerateSQL ;
import org.apache.jena.sdb.layout2.TableDescTriples ;
import org.apache.jena.sdb.sql.SDBConnection ;

public class StoreSimpleHive extends StoreBase1
{

    public StoreSimpleHive(SDBConnection connection, StoreDesc desc)
    {
        this(connection, desc, new TableDescSPO(), new CodecSimple()) ;
    }

    private StoreSimpleHive(SDBConnection connection, StoreDesc desc, TableDescTriples triples, EncoderDecoder codec)
    {
        super(connection, desc, 
              new FormatterSimpleHive(connection) ,
              new TupleLoaderSimpleHive(connection, triples, codec), 
              new QueryCompilerFactory1(codec), 
              new SQLBridgeFactory1(codec),
              new GenerateSQL(),
              triples) ;
    }
}
