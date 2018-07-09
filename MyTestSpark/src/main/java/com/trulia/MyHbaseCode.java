package com.trulia;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class MyHbaseCode {

	public static void main(String[] args) throws IOException {
		
			Configuration c = HBaseConfiguration.create(); // Instantiate Configuration class

			HTable hTable = new HTable(c, "student");       // Instantiate HTable class

			Put P1 = new Put(Bytes.toBytes("row1"));  // Instantiate put Class

			// accepts column family name, row name  and its value

			P1.add(Bytes.toBytes("college"),   Bytes.toBytes("id"),Bytes.toBytes("20"));

			P1.add(Bytes.toBytes("college"),Bytes.toBytes("name"),Bytes.toBytes("rishi"));

			hTable.put(P1);

			System.out.println("Data is inserted");       // Save the put Instance to the HTable.

			hTable.close();      // close HTable

			}

}
