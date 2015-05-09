

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;

import com.sparta.spear.rya.indexing.accumulo.ConfigUtils;
import com.sparta.spear.rya.indexing.accumulo.geo.GeoMesaGeoIndexer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.geotools.feature.SchemaException;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.ntriples.NTriplesParser;

public class GeoMesaIndexingIngest {

	private static final Logger LOGGER = Logger.getLogger(GeoMesaIndexingIngest.class.getName());
	private static final String GEO_PREDICATES = "http://www.opengis.net/ont/geosparql#asWKT";

	private static Configuration getConfig(String accumuloInstance, String zookeeper, String user, String pw, String tableName,
			String numPartitions) {

		Configuration conf = new Configuration();

		conf.set(ConfigUtils.CLOUDBASE_INSTANCE, accumuloInstance);
		conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, zookeeper);
		conf.set(ConfigUtils.CLOUDBASE_USER, user);
		conf.set(ConfigUtils.CLOUDBASE_PASSWORD, pw);
		conf.set(ConfigUtils.GEO_TABLENAME, tableName);
		conf.set(ConfigUtils.GEO_NUM_PARTITIONS, numPartitions);

		conf.set(ConfigUtils.GEO_PREDICATES_LIST, GEO_PREDICATES);

		return conf;

	}

	public static void main(String args[]) {
		String accumuloInstance;
		String zookeepers;
		String user;
		String pw;
		String filename;

		try {
			accumuloInstance = args[0];
			zookeepers = args[1];
			user = args[2];
			pw = args[3];
			filename = args[4];
		}
		catch (ArrayIndexOutOfBoundsException e) {
			String errMessage = "Not enough arguments. Necessary paramaters are 'instanceName zookeepers user pw filename'";
			throw new RuntimeException(errMessage);
		}

		String tableName = "geo";
		String numPartitions = "2";

		GeoMesaGeoIndexer indexer = null;

		try {
			File file = new File(filename);
			InputStream data = new FileInputStream(file);
			InputStreamReader inputstreamreader = new InputStreamReader(data);

			LOGGER.info("Parsing the input file");
			NTriplesParser parser = new NTriplesParser();
			StatementCollector handler = new StatementCollector();
			parser.setRDFHandler(handler);
			parser.parse(inputstreamreader, "test");

			Collection<Statement> openRdfStmts = handler.getStatements();

			LOGGER.info("Getting the GeoMesa Conifguration");
			Configuration conf = getConfig(accumuloInstance, zookeepers, user, pw, tableName, numPartitions);

			// Create GeoMesaGeoIndexer from the provided configuration and store the statements into accumulo
			LOGGER.info("Constructing the GeoMesaGeoIndexer");
			indexer = new GeoMesaGeoIndexer(conf);
			LOGGER.info("Storing the Statements");
			indexer.storeStatements(openRdfStmts);
			LOGGER.info("GeoMesa Ingest Completed");
		}
		catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("IOException: " + e.getMessage());
		}
		catch (AccumuloException e) {
			e.printStackTrace();
			throw new RuntimeException("AccumuloException: " + e.getMessage());
		}
		catch (AccumuloSecurityException e) {
			e.printStackTrace();
			throw new RuntimeException("AccumuloSecurityException: " + e.getMessage());
		}
		catch (TableNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException("Table Not Found: " + e.getMessage());
		}
		catch (SchemaException e) {
			e.printStackTrace();
			throw new RuntimeException("SchemaException: " + e.getMessage());
		}
		catch (RDFParseException e) {
			e.printStackTrace();
			throw new RuntimeException("RDFParseException: " + e.getMessage());
		}
		catch (RDFHandlerException e) {
			e.printStackTrace();
			throw new RuntimeException("RDFHandlerException: " + e.getMessage());
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Unexpected Exception occured: " + e.getMessage());
		}
		finally {
			if (indexer != null) {
				try {
					indexer.close();
				}
				catch (IOException e) {
					e.printStackTrace();
					throw new RuntimeException("Error closing connection: " + e.getMessage());
				}
			}
		}

	}

}
