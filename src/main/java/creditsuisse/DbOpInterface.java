package creditsuisse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DbOpInterface {
	void dropTable(String tableName);
	void selectTable(String tableName);
	void insertData(Dataset<Row> csv);
	
}
