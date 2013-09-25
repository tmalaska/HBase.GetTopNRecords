package com.cloudera.sa.hbase.topN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseTopNGetting {
  public static void main(String[] args) throws IOException, InterruptedException {

    if (args.length == 0 || args[0].toLowerCase().startsWith("-h")) {
      System.out.println("HBaseTop5Getting put <tableName> <columnFamily> <numberOfMSecondsIntervals> <repeatNTimes> <numberOfItems> <TopNToRetain> ");
      System.out.println("HBaseTop5Getting get <tableName> <columnFamily> <numberOfMSecondsIntervals> <repeatNTimes> <TopNToRetain>");
      return;
    }

    if (args[0].equals("get")) {
      String tableName = args[1];
      String columnFamily = args[2];
      int waitInternalTime = Integer.parseInt(args[3]);
      int repeatNTimes = Integer.parseInt(args[4]);
      int topNToRetain = Integer.parseInt(args[5]);
      

      getAction(tableName, columnFamily, waitInternalTime, repeatNTimes);
    } else {
      String tableName = args[1];
      String columnFamily = args[2];
      int waitInternalTime = Integer.parseInt(args[3]);
      int repeatNTimes = Integer.parseInt(args[4]);
      int numberOfItems = Integer.parseInt(args[5]);
      int topNToRetain = Integer.parseInt(args[6]);

      repeatingPuttingAction(tableName, columnFamily, waitInternalTime, repeatNTimes, numberOfItems, topNToRetain);
    }
  }

  private static void getAction(String tableName, String columnFamily, int waitInternalTime, int repeatNTimes) throws IOException, InterruptedException {

    Configuration hConfig = HBaseConfiguration.create();
    HTable hTable =  new HTable(hConfig, tableName);
    
    
    for (int repeat = 0; repeat < repeatNTimes; repeat++) {
      System.out.println("---Getting:{timestamp:" + System.currentTimeMillis() + ", repeat:" + repeat +"}" );
      Get get = new Get(Bytes.toBytes("topn"));
      get.setMaxVersions(5);
      Result r = hTable.get(get);
      
      KeyValue currentColumnKv = r.getColumnLatest(Bytes.toBytes(columnFamily), Bytes.toBytes("Current"));
      
      System.out.println("-Current:" + Bytes.toLong(currentColumnKv.getValue()));
      
      List<KeyValue> topNKv = r.getColumn(Bytes.toBytes(columnFamily), currentColumnKv.getValue());
      
      for (KeyValue kv: topNKv) {
        System.out.println("--" + Bytes.toString(kv.getValue()) + ":" + kv.getTimestamp());
      }
      
      Thread.sleep(waitInternalTime);
    }
    hTable.close();
    
  }

  public static void repeatingPuttingAction(String tableName, String columnFamily, int waitInternalTime, int repeatNTimes, int numberOfItems, int topNToRetain) throws IOException,
      InterruptedException {

    Configuration hConfig = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(hConfig);
    HTable hTable = null;
    
    
    if (admin.tableExists(Bytes.toBytes(tableName)) == false) {
      HTableDescriptor newTable = new HTableDescriptor();
      newTable.setName(Bytes.toBytes(tableName));

      HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
      hColumnDescriptor.setMaxVersions(topNToRetain);
      
      newTable.addFamily(hColumnDescriptor);
      admin.createTable(newTable);
    }
    hTable = new HTable(hConfig, tableName);

    long oldTimeStamp = -1;

    for (int repeat = 0; repeat < repeatNTimes; repeat++) {

      long timeStamp = System.currentTimeMillis();
      
      System.out.println("---Putting:{timeStamp:" + timeStamp + ", repeat:" + repeat +"}");
      ArrayList<Put> puts = new ArrayList<Put>();

      

      Put put = new Put(Bytes.toBytes("topn"));
      for (int i = 0; i < numberOfItems; i++) {
        
        long itemNAmont = (long) Math.abs((Math.random() * 2000));
        
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(timeStamp), itemNAmont, Bytes.toBytes("Item:" + i));
        
        System.out.println("--Item:" + i + ":" + itemNAmont);
        
      }
      put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("Current"), Bytes.toBytes(timeStamp));
      puts.add(put);

      hTable.put(puts);

      if (oldTimeStamp > -1) {
        Delete delete = new Delete(Bytes.toBytes("topn"));
        delete.deleteColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(timeStamp));
        hTable.delete(delete);
      }

      Thread.sleep(waitInternalTime);
    }
    hTable.close();

  }

}
