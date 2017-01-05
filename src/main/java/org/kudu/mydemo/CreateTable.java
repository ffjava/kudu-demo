package org.kudu.mydemo;

import java.util.LinkedList;
import java.util.List;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

/**
 * 创建表操作示例
 * @author xie_yh
 *
 */
public class CreateTable {
	
	private static ColumnSchema newColumn(String name,Type type,boolean iskey){
		ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
		column.key(iskey);
		return column.build();
	}
	
	public static void main(String[] args) throws KuduException{
		
		//master地址
		final String masteraddr = "192.168.241.128";
		
		//创建kudu的数据库链接
		KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).build();
		
		//设置表的schema
		List<ColumnSchema> columns = new LinkedList<>();
		columns.add(newColumn("CompanyId",Type.INT16,true));
		columns.add(newColumn("WorkId",Type.INT32,true));
		columns.add(newColumn("Gender",Type.STRING,false));
		columns.add(newColumn("Desc",Type.STRING,false));
		columns.add(newColumn("Photo",Type.BINARY,false));
		
		Schema schema = new Schema(columns);
		CreateTableOptions options = new CreateTableOptions();
		
		//设置表的replica备份和分区规则
		List<String> parcols = new LinkedList<>();
		parcols.add("CompanyId");
		
		//一个replica
		options.setNumReplicas(1);
		//用列companyid做为分区的参照
		options.setRangePartitionColumns(parcols);
		
		try {
			client.createTable("PERSON", schema, options);
		} catch (KuduException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		client.close();
	}

}
