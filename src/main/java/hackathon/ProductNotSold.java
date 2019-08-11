package hackathon;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class ProductNotSold {
	public static void main(String[] args) throws Exception {
	Logger.getLogger("org").setLevel(Level.ERROR);

    SparkSession session = SparkSession.builder().appName("UkMakerSpaces").master("local[*]").getOrCreate();
    Dataset<Row> product = session.read().option("header", "true").option("delimiter", "|").option("inferSchema", "true").csv("hackatron/Product.txt");
    Dataset<Row> sales = session.read().option("header", "true").option("delimiter", "|").option("inferSchema", "true").csv("hackatron/Sales.txt");
	
    Dataset<Row> never_sold = sales.join(product,
            sales.col("product_id").equalTo(product.col("product_id")), "left_outer").filter(sales.col("product_id").isNotNull());
    System.out.println("=== Print 20 records of resultant table Q#5 ===");
    
    never_sold.createOrReplaceTempView("neversold");
    
    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);
    
    sqlContext.sql("select product_name from neversold").show();
    
    
    
	}
}

