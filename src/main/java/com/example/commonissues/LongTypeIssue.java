
package com.example.commonissues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class LongTypeIssue {
    
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("CreateTable")
                .getOrCreate();
        
         List<Row> list = new ArrayList<>();
        list.add(RowFactory.create(1L));
        list.add(RowFactory.create( 1L + (Integer.MAX_VALUE)));
        list.add(RowFactory.create( 1L + (Integer.MAX_VALUE) + (Integer.MAX_VALUE)));
        
        
        StructType structType = DataTypes.createStructType(
                Arrays.asList( DataTypes.createStructField("longVal", DataTypes.LongType, true) )
        );
        Dataset<Row> dataset1 = spark.createDataFrame(list, structType);
        dataset1.show();
        
        dataset1.printSchema();
        dataset1.createOrReplaceTempView("dataset1");
        
        Dataset<Row> dataset2 = spark.sql(
              """
              SELECT longVal, (longVal > 0) condition1, (longVal > 0L) condition2  FROM (
                  SELECT longVal FROM dataset1 UNION select ''
              ) tab 
              """
              );
        dataset2.printSchema();
        dataset2.show();
        
        Dataset<Row> dataset3 = spark.sql(
                """
                SELECT longVal, (longVal > 0) condition1, (longVal > 0L) condition2 FROM (
                    SELECT longVal FROM dataset1 UNION select 0 
                ) tab
                """
                );
        dataset3.printSchema();
        dataset3.show();
      

        
    }
    
    
}
