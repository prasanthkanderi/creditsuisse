package creditsuisse;


import org.apache.spark.SparkConf;
//import org.apache.spark.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class DataApp {

public static void main(String args[]){

	Logger logger = LoggerFactory.getLogger(DataApp.class);
    logger.info("Starting the Session");
	SparkConf conf = new SparkConf().setMaster("local").setAppName("SaavnAnalyticsProject");
	SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
	Dataset<Row> start_df=null;

    try {
    	
    	Dataset<Row> csv = spark.read().format("json").option("header","true").load(args[0]);
    	logger.info("Converted Logfile.txt to Dataset");
    	
    	logger.debug("The logfile content is:");
    	csv.show();
    	
    	start_df = csv.filter("state=='STARTED'").withColumnRenamed("timestamp", "start");
    	Dataset<Row> end_df = csv.filter("state=='FINISHED'").withColumnRenamed("timestamp", "finish").select("id", "finish");
    	start_df = start_df.join(end_df, "id").withColumn("duration", end_df.col("finish").minus(start_df.col("start")));
    	start_df = start_df.select("id", "host", "type", "duration").withColumn("Alert", start_df.col("duration").gt(4));
    	
    	logger.info("---------The resultant Dataset is----");
    	start_df.show();
    	
    } catch (Exception e) {
    	logger.error("exception occuerred", e.fillInStackTrace());
    }

    new AppDAO("LogTable", start_df).performDAO();
    
    logger.info("Project Completed");
    
    
  }
}