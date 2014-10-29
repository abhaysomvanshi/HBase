/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//package impatient;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


//.HBase.Configuration;



public class
  Main
  {
  public static void
  main( String[] args ) throws IOException
    {
	  Configuration config = HBaseConfiguration.create();
	// Create Instance of Hbase Admin by passing config detail
	 HBaseAdmin hbAdmin = new HBaseAdmin(config);
	 //Creating Instance of Hbase table descriptor by specifyimng the expected table name 
	 // In our case it is HbaseFirstTable
	 HTableDescriptor hbHtd = new HTableDescriptor("HbaseFirstTable");
	 // Defining column description cfdata 
	 HColumnDescriptor hcd = new HColumnDescriptor("cfdata");
	 // adding column description cfdata to table descriptor hbHtd
	 hbHtd.addFamily(hcd);
	 // Below line i used to test the existing table in hbase, however it is not use so commenting those lines.

	 hbAdmin.createTable(hbHtd);

	  byte [] tablename = hbHtd.getName();
	  HTableDescriptor [] tables = hbAdmin.listTables();
	 
	// Run some operations -- a put, a get, and a scan -- against the table.
	  HTable table = new HTable(config, tablename);
	  byte [] row1 = Bytes.toBytes("row1");
	  Put objput = new Put(row1);
	  byte [] databytes = Bytes.toBytes("cfdata");
	  objput.add(databytes, Bytes.toBytes("1"), Bytes.toBytes("NewValue"));
	  table.put(objput);
	  
	  //Adding second row
	  byte [] row2 = Bytes.toBytes("row2");
	  objput=new Put(row2);
	  objput.add(databytes, Bytes.toBytes("2"), Bytes.toBytes("NewValue2"));
	  table.put(objput);
	  
	  Get objget = new Get(row1);
	  Result result = table.get(objget);
	  System.out.println("Get: " + result);
	  Scan scan = new Scan();
	  ResultScanner scanner = table.getScanner(scan);

	  try {
		  for (Result scannerResult: scanner) {
		  System.out.println("Scan: " + scannerResult);
		  }
		  } finally {
		  scanner.close();
		  }
		  
    }
  }
