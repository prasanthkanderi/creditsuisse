package creditsuisse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppDAO {
	Logger logger = LoggerFactory.getLogger(AppDAO.class);
	private DbOpInterface dbOperations;
	private String table;
	private Dataset<Row> df;
	
	public AppDAO(String table, Dataset<Row> df)
	{
		this.table = table;
		this.df = df;
	}
	
	public void performDAO()
	{
		
	    logger.info("Performing required Database operations");
	    
		dbOperations=new DBOperations();
		System.out.println("---------------"+table);
		
  /*****	Required Operations  *****/	
//		dbOperations.dropTable(table);
		
		dbOperations.insertData(df);
		dbOperations.selectTable(table);
		logger.info("Performed required Database operations");
	}
}