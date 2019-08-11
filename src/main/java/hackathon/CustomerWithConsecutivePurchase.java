package hackathon;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class CustomerWithConsecutivePurchase {
	public static void main(String[] args) throws Exception {
		Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().appName("UkMakerSpaces").master("local[*]").getOrCreate();
       Dataset<Row> sales = session.read().option("header", "true").option("delimiter", "|").option("inferSchema", "true").csv("hackatron/Sales.txt");
	  sales.createOrReplaceTempView("sales");
	
	
	SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);
	
	Dataset<Row> temp = sqlContext.sql("select  customer_id,count(product_id),count(transaction_id),\n" + 
			"from_unixtime(unix_timestamp(timestamp,'MM/dd/yyyy'),'yyyy-MM-dd') as saleDate from sales group by "
			+ " customer_id,from_unixtime(unix_timestamp(timestamp,'MM/dd/yyyy'),'yyyy-MM-dd') having count(product_id)>=2 and "
			+ "count(transaction_id)>=2"
			);
	
    temp.createOrReplaceTempView("Cust");
    sqlContext.sql("select count(customer_id) from Cust").show();

	}
}


