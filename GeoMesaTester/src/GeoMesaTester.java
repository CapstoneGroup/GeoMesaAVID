/**
 * New and improved GeoMesaTester
 * 
 * GeoMesaTester utilizes the GeoMesaGeoIndexer to index and then query RDF data.
 **/

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.rdftriplestore.RdfCloudTripleStore;
import mvm.rya.rdftriplestore.RyaSailRepository;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.geotools.geometry.jts.JTS;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;

import com.sparta.spear.rya.indexing.StatementContraints;
import com.sparta.spear.rya.indexing.accumulo.ConfigUtils;
import com.sparta.spear.rya.indexing.accumulo.geo.GeoConstants;
import com.sparta.spear.rya.indexing.accumulo.geo.GeoMesaGeoIndexer;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;

public class GeoMesaTester 
{
	protected static String instanceName = "accumuloInstance";
	protected static String zooKeepers = "192.168.1.103:2181";
	protected static StatementContraints constraints = new StatementContraints();
	protected static RdfCloudTripleStore store;
	protected static ValueFactory vf;
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) 
	{
	    try 
	    {
	    	Configuration conf = new Configuration();
	    		
	    	conf.set(ConfigUtils.CLOUDBASE_INSTANCE, instanceName);
	    	conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, zooKeepers);
	    	conf.set(ConfigUtils.CLOUDBASE_USER, "root");
	    	conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "secret");
	    	conf.set(ConfigUtils.GEO_TABLENAME, "sometable");
	    	conf.set(ConfigUtils.GEO_NUM_PARTITIONS, "2");
	    	
	    	conf.set(ConfigUtils.GEO_PREDICATES_LIST, "http://mynamespace/hasCoords");
	    	
	    	// get the dataStore, valueFactory, and create the URIs for our statement
	    	store = getRdfStore();
	    	vf = store.getValueFactory();
	    	URI s = vf.createURI("http://mynamespace/places#location");
	    	URI p = vf.createURI("http://mynamespace/hasCoords");
	    	GeoMesaGeoIndexer indexer = getIndexer(conf);
	    	
	    	//GeometryFactory geoFac = new GeometryFactory();
	        //final CoordinateSequenceFactory sequence=factory.getCoordinateSequenceFactory();
	    	//CoordinateSequenceFactory csFactory=(CoordinateSequenceFactory)hints.get(Hints.JTS_COORDINATE_SEQUENCE_FACTORY);	    	
	    	//final Geometry geometry=new Geometry(new Rectangle2D.Double(0.0,0.0,2.0,2.0));
	    	//CoordinateSequenceFactory csFac = new CoordinateSequenceFactory();
	    	//Coordinate[] array=new Coordinate[]{new Coordinate(-90,-90),new Coordinate(90, 90)};
	    	//CoordinateArraySequence seq = new CoordinateArraySequence(array);
	    	//Coordinate[] array=new Coordinate[]{new Coordinate( 86, 41.25)};
	    	//Geometry geo = new Point(seq, geoFac);
	    	//LinearRing lr = new LinearRing(seq, geoFac);
	    	//Geometry geo = new Polygon(lr, null, geoFac);
	    	//CoordinateSequence sequence=  csFac.create(array);
	    	
	    	// set up the geometry constraints
	    	Envelope WORLD = new Envelope(-100, 100, -100, 100);
	    	Geometry geo = JTS.toGeometry(WORLD);
	    	
	    	// run query
	    	CloseableIteration<Statement, QueryEvaluationException> results = indexer.queryContains(geo, constraints);
	    
	    	System.out.println(results.hasNext());
	    	while (results.hasNext()) 
	    	{
	    		System.out.println(results.next());
	    	}
	    	
	    	
	    	System.out.println("Got here");
	    } 
	    catch (Exception e) 
	    {
	        e.printStackTrace();
	    }
	}
	
	
	/**
	 * 
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	private static GeoMesaGeoIndexer getIndexer(Configuration conf) throws IOException
	{
		try
		{
	    	System.out.println("BEGIN GEOMESA");
	    	GeoMesaGeoIndexer indexer = new GeoMesaGeoIndexer(conf);
	    	
	    	
	    	// create the RFD subject, predicate, and object
	    	URI s = vf.createURI("http://mynamespace/places#location");
	    	URI p = vf.createURI("http://mynamespace/hasCoords");
	    	Literal o = vf.createLiteral("POINT(50 50)", GeoConstants.XMLSCHEMA_OGC_WKT);
	    	//Literal o = vf.createLiteral("POINT(40, 90)");
	    	//Literal o = vf.createLiteral("POLYGON((100 -50, 100 100, -100 100, -100 -100, 100 -50))");
	    	
	    	// create and store the statement, creating a feature in Accumulo
	    	Statement stmt = vf.createStatement(s, p, o);
	    	System.out.println(stmt.toString());
	    	System.out.println(store.toString());
	    	indexer.storeStatement(stmt);
	    	
	    	// set the predicate and subject constraints for the query
	    	Set<URI> set = new HashSet();
	    	set.add(p);
	    	constraints = new StatementContraints().setPredicates(set);
	    	constraints.setSubject(s);
	    	
	    	return indexer;
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	private static boolean getAccumuloConnection() throws AccumuloException, AccumuloSecurityException, TableNotFoundException
	{
		try
		{
			//Accumulo Testing
		
	    	System.out.println("BEGIN ACCUMULO");
	    	Instance inst = new ZooKeeperInstance(instanceName, zooKeepers);
	    	Connector conn = inst.getConnector("root", new PasswordToken("secret"));
	    	Authorizations auths = new Authorizations("public");
	    	
	    	Scanner scan = conn.createScanner("sometable", auths);
	    	scan.setRange(new Range("harry", "john"));
	    	scan.fetchColumnFamily(new Text("attributes"));
	    	
	    	return true;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * 
	 * @return
	 * @throws Exception
	 */
	private static RdfCloudTripleStore getRdfStore() throws Exception 
	{
        final RdfCloudTripleStore store = new RdfCloudTripleStore();
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        AccumuloRyaDAO dao = new AccumuloRyaDAO();
        
        Connector connector = new ZooKeeperInstance(instanceName, zooKeepers).getConnector("root", "secret");
        dao.setConnector(connector);
        conf.setTablePrefix("triplestore_");
        dao.setConf(conf);
        store.setRyaDAO(dao);
        
        return store;
	}
}
