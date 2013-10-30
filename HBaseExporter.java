package hbaseexport;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

public class HBaseExporter extends HBaseImportExport{
		
		String webpage;
	
	/**
	 * Construct an exporter for an HBase table
	 * 
	 * @param webpage name of the table in HBase
	 */
	HBaseExporter(String webpage) {
		this.webpage = webpage;
	}
	/**
	 * @param args
	 */
	public void exportCSV(File csvFilePath) throws IOException {
		Configuration config = HBaseConfiguration.create(); 
		
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor desc = admin.getTableDescriptor(webpage.getBytes());
		
		OutputStream os = new FileOutputStream(csvFilePath);
		CsvOutputStream cos = new CsvOutputStream(os);
		cos.setEncoding("UTF-8");
		cos.setDelimiter(',');
		
		HTable table = new HTable(config, webpage);
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("p")); 
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        
        int counter = 0;
        
        List<HBaseCol> columns = new ArrayList<HBaseCol>(); 
        List<String> values = new ArrayList<String>();
        
        Logger logger = org.apache.log4j.Logger.getLogger(this.getClass());
        
        while((result=resultScanner.next()) != null) {
        	
        	values.clear();
        	
        	if (columns.size() == 0) {
        		for(KeyValue key: result.list()) {
        			String family = new String(key.getFamily(),"UTF-8");
        			String qualifier = new String(key.getQualifier(),"UTF-8");
            		columns.add(new HBaseCol(family,qualifier));
            		values.add(family+":"+qualifier);
            	}
        		cos.writeLine(values.toArray(new String[0]));
        		values.clear();
        	}
        	
        	for(HBaseCol column: columns) {
        		byte[] val = result.getValue(column.family.getBytes("UTF-8"), column.qualifier.getBytes("UTF-8"));
        		if (val != null) {
        			values.add(new String(val,"UTF-8"));
        		} else {
        			values.add("");
        		}
        	}
        	
        	cos.writeLine(values.toArray(new String[0]));
        	counter += 1;
			if (counter % 10000 == 0) {
				logger.info("Exported "+counter+" records");
			}
        }
        cos.close();
	}
	
		public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		if (args.length != 2){
			System.out.println("Usage: java -/usr/lib/jvm/java-6-openjdk-i386 HBaseExporter webpage /home/ishva/Project/Hbase/mydata.csv");
		}
		
		String webpage = "webpage";
		String filePath = "/home/ishva/Project/Hbase/mydata.csv";
		File csvOutputFile = new File(filePath);
		HBaseExporter exporter = new HBaseExporter(webpage);
		exporter.exportCSV(csvOutputFile);
	}

}
