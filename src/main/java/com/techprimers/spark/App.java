package com.techprimers.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.types.StructType;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) throws InterruptedException {

        SparkSession session = SparkSession
                .builder()
                .appName("SparkJavaExample")
                .master("local[3]")
                .getOrCreate();

        StructType schema = new StructType()
                //.add("Transaction_date", "Date")
                .add("Product", "string")
                .add("Price", "long")
                .add("Payment_Type", "string")
                .add("Name", "string")
                .add("City", "string")
                .add("State", "string")
                .add("Country", "string")
                //.add("Account_Created", "Date")
                //.add("Last_Login", "Date")
                .add("Latitude", "long")
                .add("Longitude", "long");

        Dataset<org.apache.spark.sql.Row> df = session.read()
                .option("mode", "DROPMALFORMED")
                .schema(schema)
                //.option("dateFormat", "MM/dd/yyyy h:MM")
                //.csv("D:\\Soluciones\\__data-machineLearnig\\__data\\SalesJan2009.csv");
                .csv("file:///home/osboxes/Downloads/SalesJan2009.csv");

        System.out.println("Total registros: " + df.count());

        df.createOrReplaceTempView("ventas");
        //Dataset<Row> sqlResult = spark.sql("SELECT InvoiceNo, InvoiceDate, CustomerID, SUM( UnitPrice*Quantity ) FROM pedidos GROUP BY InvoiceNo,InvoiceDate,CustomerID ORDER BY 4 DESC LIMIT 10");
        Dataset<org.apache.spark.sql.Row> sqlResult = session.sql("SELECT Product, SUM( Price*100 ),Latitude,Longitude FROM ventas GROUP BY  Product,Latitude,Longitude ORDER BY 1 DESC LIMIT 10");

        sqlResult.show();

        sqlResult.write().option("header", "true").csv("file:///home/osboxes/Downloads/out");
        /* try (JavaSparkContext context = new JavaSparkContext(session.sparkContext())) {
         List<Integer> integers = Arrays.asList(1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

         JavaRDD<Integer> javaRDD = context.parallelize(integers, 3);

         javaRDD.foreach((VoidFunction<Integer>) integer -> {
         System.out.println("Java RDD:" + integer);
         Thread.sleep(3000);
         });

         Thread.sleep(1000000);
         context.stop();
         }*/
    }
}
