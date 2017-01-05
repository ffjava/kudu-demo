package org.kudu.mydemo;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSortScanerBuilder;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;

/**
 * 按条件搜索
 * @author xie_yh
 *
 */
public class FindRow {
	public static void main(String[] args) throws KuduException {

		//master地址
		final String masteraddr = "192.168.241.128";

		//创建kudu的数据库链接
		KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).build();

		//打开表
		KuduTable table = client.openTable("PERSON");
		
		/**
		 * 设置搜索的条件
		 */
		KuduScannerBuilder builder = client.newScannerBuilder(table);
		KuduPredicate predicate = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("CompanyId"),
				ComparisonOp.EQUAL, 1);
		builder.addPredicate(predicate);
		
		// 设置排序
		KuduSortScanerBuilder.setSort(builder);

		// 开始扫描
		KuduScanner scaner = builder.build();
		while (scaner.hasMoreRows()) {
			RowResultIterator iterator = scaner.nextRows();
			while (iterator.hasNext()) {
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
