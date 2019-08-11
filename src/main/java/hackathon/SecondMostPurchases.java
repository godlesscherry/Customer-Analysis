package hackathon;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;


public class SecondMostPurchases {
	public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().appName("UkMakerSpaces").master("local[*]").getOrCreate();
        Dataset<Row> sales = session.read().option("header", "true").option("delimiter", "|").option("inferSchema", "true").csv("hackatron/Sales.txt");
    	sales.createOrReplaceTempView("sales");
    	Dataset<Row> customer = session.read().option("header", "true").option("delimiter", "|").option("inferSchema", "true").csv("hackatron/Customer.txt");
    	customer.createOrReplaceTempView("customer");
    	Dataset<Row> refund = session.read().option("header", "true").option("delimiter", "|").option("inferSchema", "true").csv("hackatron/Refund.txt");
    	customer.createOrReplaceTempView("refund");
    	
    	
    	
    	SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);
    	
    	
    	
    	
    	Dataset<Row> cust = sqlContext.sql("select transaction_id,  customer_id, month(from_unixtime(unix_timestamp(timestamp,'MM/dd/yyyy'),'yyyy-MM-dd')) as monthOutput"
    			+" , year(from_unixtime(unix_timestamp(timestamp,'MM/dd/yyyy'),'yyyy-MM-dd')) as YearOutput from sales");
    			
    	cust.createOrReplaceTempView("cust");
    	
    	Dataset<Row> custom = sqlContext.sql("select * from  cust  where YearOutput = 2013 and monthOutput =5");
    	
    	custom.createOrReplaceTempView("c2");
    	Dataset<Row> custom2 = sqlContext.sql("select * from ( select transaction_id, customer_id, ROW_NUMBER() "
    			+ "over (ORDER BY transaction_id) as row_no from c2 group by customer_id,Transaction_id) res "
    			+ "where res.row_no = 2  ");
    	
    	//custom2.show();
    	

   Dataset<Row> result = custom2.join(customer,
                         custom2.col("customer_id").equalTo(customer.col("customer_id")));
   
   result = result.select(
		    result.col("customer_first_name"),
		    result.col("customer_last_name"));
   result.show();
        
    	
    	
	}
        

}
