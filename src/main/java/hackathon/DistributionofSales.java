package hackathon;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class DistributionofSales {
	public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().appName("UkMakerSpaces").master("local[*]").getOrCreate();
        Dataset<Row> product = session.read().option("header", "true").option("delimiter", "|").option("inferSchema", "true").csv("hackatron/Product.txt");
        Dataset<Row> sales = session.read().option("header", "true").option("delimiter", "|").option("inferSchema", "true").csv("hackatron/Sales.txt");
        //System.out.println("=== Print 20 records of makerspace table ===");
       // product.show();

        //System.out.println("=== Print 20 records of product table ===");
        //sales.show();
        Dataset<Row> sales_product = sales.join(product,
                sales.col("product_id").equalTo(product.col("product_id")));
        
        //System.out.println("=== Print 20 records of sales table ===");
        //sales_product.show();
        
       sales_product = sales_product.select(
        		sales_product.col("product_name"),
        		sales_product.col("product_type"),
        		sales_product.col("total_quantity"));
      // System.out.println("=== Print 20 records of resultant table ===");
       //sales_product.show();
       System.out.println("=== Print 20 records of resultant table Q#2 ===");
       sales_product.groupBy("product_name","product_type").agg(functions.sum("total_quantity")).show();
       

        		
        
	}
}
