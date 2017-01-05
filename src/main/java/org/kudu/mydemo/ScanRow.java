package org.kudu.mydemo;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSortScanerBuilder;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

/**
 * 按主键扫描
 * @author xie_yh
 *
 */
public class ScanRow {
	public static void main(String[] args) throws KuduException {

		//master地址
		final String masteraddr = "192.168.241.128";

		//创建kudu的数据库链接
		KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).build();

		//打开表
		KuduTable table = client.openTable("PERSON");
		
		/**
		 * 设置搜索的schema
		 */
		KuduScannerBuilder builder = client.newScannerBuilder(table);
		PartialRow startrow = table.getSchema().newPartialRow();
		PartialRow endrow = table.getSchema().newPartialRow();
		
		/**
		 * 填写起始行和结束行
		 */
		startrow.addShort("CompanyId", (short) 1);
		startrow.addInt("WorkId", 1);
		
		endrow.addShort("CompanyId", (short) 1);
		endrow.addInt("WorkId", Short.MAX_VALUE);
		builder.lowerBound(startrow);
		builder.exclusiveUpperBound(endrow);
		
		//设置排序
		KuduSortScanerBuilder.setSort(builder);
		
		//开始扫描
		KuduScanner scaner = builder.build();
		while(scaner.hasMoreRows()){
			RowResultIterator iterator = scaner.nextRows();
			while(iterator.hasNext()){
				RowResult result = iterator.next();
				/**
				 * 输出行
				 */
				System.out.println("CompanyId:" + result.getShort("CompanyId"));
				System.out.println("WorkId:" + result.getInt("WorkId"));
				System.out.println("Gender:" + result.getString("Gender"));
				System.out.println("Desc:" + result.getString("Desc"));
			}
		}
		
		scaner.close();
		client.close();
	}

}
