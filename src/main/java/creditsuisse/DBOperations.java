package creditsuisse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DBOperations implements DbOpInterface{
	Connection con = null;
	Statement stmt = null;
	Logger logger = LoggerFactory.getLogger(DBOperations.class);
    
	public DBOperations()
	{
		
	      try {
	         //Registering the HSQLDB JDBC driver
	         Class.forName("org.hsqldb.jdbc.JDBCDriver");
	         //Creating the connection with HSQLDB
	         System.out.println("jdbc class imported");
	         con = DriverManager.getConnection("jdbc:hsqldb:file:logdb", "SA", "");
	         stmt = con.createStatement();
	         if (con!= null){
	        	 logger.info("Connection created successfully");

	         }else{
	        	 logger.error("Problem with creating connection");
	         }
	      
	      }  catch (Exception e) {
	    	  logger.error("exception occuerred", e.fillInStackTrace());
	      }
	}
	public void createTable(String table_name)
	 {
		logger.info("Crete table if it is already not created");
		 try {
			
			 stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +table_name+"(id VARCHAR(50) NOT NULL,"
				 		+ "host VARCHAR(10),"
				 		+ "type VARCHAR(30),"
				 		+ "duration int NOT NULL,"
				 		+ "Alert bit);");
			 
		} catch (SQLException e) {
			logger.error("exception occuerred", e.fillInStackTrace());
		}

	 }

	@Override
	public void dropTable(String tableName)
	{
    try {
		
	    String sql = "DROP TABLE IF EXISTS "+tableName;
	    stmt.executeUpdate(sql);
	    System.out.println("Table dropped");
	} catch (SQLException e) {
		logger.error("exception occuerred", e.fillInStackTrace());
	}

    
	}
	
	@Override
	public void selectTable(String tableName)
	 {

    ResultSet result;
	try {
		result = stmt.executeQuery("SELECT * FROM "+tableName);
		while(result.next()){
		       System.out.println(result.getString("id")+" | "+
		    	 " | "+ result.getString("host") + " | " + result.getString("type")+
		    	 " | "+ result.getString("duration") + " | "+ result.getString("Alert"));
		       
	} }
		catch (SQLException e) {
			logger.error("exception occuerred", e.fillInStackTrace());
	}
    
       
    }
	 

	@Override
	public void insertData(Dataset<Row> csv)
	{

		try
		{
			 createTable("LogTable");
			 Properties connectionProperties = new Properties();
			 connectionProperties.put("driver", "org.hsqldb.jdbc.JDBCDriver");
			 connectionProperties.put("url", "jdbc:hsqldb:file:logdb");
			 connectionProperties.put("user", "SA");
			 connectionProperties.put("password", "");

			 csv.write().mode(SaveMode.Append).jdbc(connectionProperties.getProperty("url"), "LogTable", connectionProperties);
			 logger.info("Data Inserted in database:logdb");
 
		 }
		catch(Exception e)
		{
			logger.error("exception occuerred", e.fillInStackTrace());
		}

	}
	
}
