package org.kudu.mydemo;

import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;

/**
 * kudu表写入示例
 * @author xie_yh
 *
 */
public class InsertRow {
	public static void main(String[] args) throws KuduException {

		//master地址
		final String masteraddr = "192.168.241.128";

		//创建kudu的数据库链接
		KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).build();

		//打开表
		KuduTable table = client.openTable("PERSON");
		Insert insert = table.newInsert();

		//设置字段内容
		insert.getRow().addShort("CompanyId", (short) 1);
		insert.getRow().addInt("WorkId", 1);
		insert.getRow().addString("Gender", "male");
		insert.getRow().addString("Desc", "desc of the person");
		insert.getRow().addBinary("Photo", "person".getBytes());

		//创建写session,kudu必须通过session写入
		KuduSession session = client.newSession();
		session.apply(insert);
		
		session.close();
		client.close();
	}
}
