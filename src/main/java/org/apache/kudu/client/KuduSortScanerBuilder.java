package org.apache.kudu.client;

import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;

/**
 * kudu的主键排序功能
 * @author xie_yh
 *
 */
public class KuduSortScanerBuilder {
	
	public static KuduScannerBuilder setSort(KuduScannerBuilder builder){
		builder.sortResultsByPrimaryKey();
		return builder;
	}

}
