package hackathon;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class TotalTransactionsWithoutRefunds {
	public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().appName("UkMakerSpaces").master("local[*]").getOrCreate();
        Dataset<Row> sales = session.read().option("header", "true").option("delimiter", "|").option("inferSchema", "true").csv("hackatron/Sales.txt");
	sales.createOrReplaceTempView("sales");
	
	
	Dataset<Row> refund = session.read().option("header", "true").option("delimiter", "|").option("inferSchema", "true").csv("hackatron/Refund.txt");
	refund.createOrReplaceTempView("refund");
	
	
	SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);
	
	Dataset<Row> transaction =sqlContext.sql("select transaction_id, timestamp,total_amount,"
			+ " year(from_unixtime(unix_timestamp(timestamp,'MM/dd/yyyy'),'yyyy-MM-dd')) as YearOutput"
			+ " from sales ");
	
	transaction.createOrReplaceTempView("Transaction");
	
	sqlContext.sql("select  sum(t.total_amount) from Transaction t left outer join refund r on "
			+ "(t.transaction_id = r.original_transaction_id) "
			+ "where YearOutput=2013").show();

	}

}
