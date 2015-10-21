package scripts;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.jena.riot.RDFDataMgr;
import org.bridgedb.DataSource;
import org.bridgedb.IDMapperException;
import org.bridgedb.Xref;
import org.bridgedb.bio.DataSourceTxt;
import org.bridgedb.creator.BridgeDbCreator;
import org.bridgedb.creator.DbBuilder;


import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

//import script.GeneAttributes;

/**
 * Parse a linkset (e.g .ttl) and create a BridgeDb database
 * 
 * @author jonathan
 *
 */
public class Linkset2Bdb {
	//Contains the mapping from Ensembl to external database 
	static HashMap<Xref, HashSet<Xref>>  dbEntries = new HashMap<Xref, HashSet<Xref>>(); 	
	//Contains the gene attributes of the Ensembl gene id
	//	static HashMap<Xref, GeneAttributes>  geneSet = new HashMap<Xref, GeneAttributes>();	

	public static void main(String[] args) throws FileNotFoundException, IDMapperException, SQLException {
		// TODO Auto-generated method stub
		//Initialize BrideDb data source
		DataSourceTxt.init();		
		//Path of the directory which contains the linksets
		File dir = new File ("/home/bigcat-jonathan/LinkTest/hmdb/");
		if (dir.isDirectory()){
			FilenameFilter textFilter = new FilenameFilter() {
				public boolean accept(File dir, String name) {
					String lowercaseName = name.toLowerCase();
					if (lowercaseName.endsWith(".ttl")) {
						return true;
					} else {
						return false;
					}
				}
			};
			File[] listFiles = dir.listFiles(textFilter);

			for(File f : listFiles){
				Model model = RDFDataMgr.loadModel(f.getAbsolutePath());	
				parse(model);
			}
		}
		bdbCreate("/home/bigcat-jonathan/LinkTest/hmdb/TestHmdb");
	}


/**
 * Parse the model RDF linkset to extract all the references 
 * @param model
 */
public static void parse(Model model){
	StmtIterator list = model.listStatements();
	while (list.hasNext()){
		Statement s = list.next();
		// TODO How to handle the different predicate ?
		if (s.getPredicate().getURI().equals("http://www.w3.org/2004/02/skos/core#relatedMatch")){			
			String obj = s.getObject().toString();
			int index_obj = obj.lastIndexOf("/");
			String ref_obj = obj.substring(index_obj+1);
			String db_obj = obj.substring(0, index_obj);
//			System.out.println(db_obj+"\t"+ref_obj);
			
			String sub = s.getSubject().toString();
			int index_sub = sub.lastIndexOf("/");
			String ref_sub = sub.substring(index_sub+1);
			String db_sub = sub.substring(0, index_sub);
//			System.out.println(db_sub+"\t"+ref_sub);
			
			// TODO How to handle URIs which are not from IdentiferOrg ?
			Xref mainXref = new Xref(ref_sub, DataSource.getByIdentiferOrgBase(db_sub));
			Xref xref = new Xref(ref_obj, DataSource.getByIdentiferOrgBase(db_obj));
//			System.out.println(mainXref);
//			System.out.println(xref);
			HashSet<Xref> xrefSet = dbEntries.get(mainXref);
			if (xrefSet==null){
				HashSet<Xref> database = new HashSet<Xref>();
				database.add(xref);
				dbEntries.put(mainXref, database);
			}
			else{
				xrefSet.add(xref);
			}
		}
	}
}
/**
 * Run the creation of the database
 * @param out - Path of the output database
 * @throws IDMapperException
 * @throws SQLException
 * @throws FileNotFoundException
 */
public static void bdbCreate(String out) 
		throws IDMapperException, SQLException, FileNotFoundException{
	BridgeDbCreator creator = new BridgeDbCreator(dbEntries);
	// TODO Create a config file for all the properties 
	creator.setOutputFilePath(out);
	creator.setDbSourceName("hmdb");
	creator.setDbVersion("1");
	creator.setDbSeries("Hmdb gene & co");
	creator.setDbDataType("GeneProduct");

	DbBuilder dbBuilder = new DbBuilder(creator);
	dbBuilder.createNewDb();
//	dbBuilder.addEntry(dbEntries,geneSet);
	dbBuilder.addEntry(dbEntries);
	dbBuilder.finalizeDb();
	System.out.println(dbBuilder.getError()+" errors (duplicates) occurred"+ dbBuilder.getErrorString());

}
}
