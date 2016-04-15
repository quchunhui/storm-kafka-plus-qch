package com.dscn.helloworld.bolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.json.JSONObject;

import com.dscn.helloworld.common.Constants;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings({ "deprecation", "serial" })
public class SurfBolt extends BaseRichBolt {
	private static Configuration _conf = HBaseConfiguration.create();
	private static HTable _hTable = null;
	private OutputCollector _collector;
	private HashMap<String, String> map = new HashMap<String, String>();

	long startTime = System.currentTimeMillis();
    long count = 0;

    public SurfBolt() {
		_conf.set("hbase.zookeeper.quorum", Constants.hostList);
		try {
			_hTable = new HTable(_conf, "test");
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    @SuppressWarnings("rawtypes")
	public void prepare(Map sconf, TopologyContext context, OutputCollector collector) {
    	_collector = collector;
    }

	@SuppressWarnings("rawtypes")
	public void execute(Tuple tuple) {
    	System.out.println("SurfBolt sentence=" + tuple.getString(0));
		String jsonObject = tuple.getStringByField("JsonMsg");
		JSONObject jsonArray = new JSONObject(jsonObject);

    	count++;
        if (count % 20000 == 0) {
        	long sumTime = System.currentTimeMillis();
        	System.out.println("[RESULT]the time of 20000 is [" + (sumTime - startTime) + "]");
        }

        try {
			String row_key = jsonArray.get("logisticProviderID").toString() + ":" + jsonArray.get("mailNo").toString();
			map.put("mailType", jsonArray.get("mailType").toString());				// 面单类型
			map.put("weight", jsonArray.get("weight").toString());					// 重量
			map.put("senAreaCode", jsonArray.get("senAreaCode").toString());		// 寄件国家代码
			map.put("recAreaCode", jsonArray.get("recAreaCode").toString());		// 收件国家代码
			map.put("senCityCode", jsonArray.get("senCityCode").toString());		// 寄件城市代码
			map.put("recCityCode", jsonArray.get("recCityCode").toString());		// 收件城市代码
			map.put("senProv", jsonArray.get("senProvCode").toString());			// 寄件城市代码
			map.put("senCity", jsonArray.get("senCity").toString());				// 收件城市代码
			map.put("senCountyCode", jsonArray.get("senCountyCode").toString());	// 寄件国家代码
			map.put("senAddress", jsonArray.get("senAddress").toString());			// 收件国家代码
			map.put("senName", jsonArray.get("senName").toString());				// 寄件人姓名
			map.put("senMobile", jsonArray.get("senMobile").toString());			// 寄件人移动电话
			map.put("senPhone", jsonArray.get("senPhone").toString());				// 寄件人固定电话
			map.put("recProvCode", jsonArray.get("recProvCode").toString());		// 收件人姓名
			map.put("recCity", jsonArray.get("recCity").toString());				// 收件人移动电话
			map.put("recCountyCode", jsonArray.get("recCountyCode").toString());	// 收件人固定电话
			map.put("recAddress", jsonArray.get("recAddress").toString());			// 收件地址省份
			map.put("recName", jsonArray.get("recName").toString());				// 收件地址城市
			map.put("recMobile", jsonArray.get("recMobile").toString());			// 收件地址区县
			map.put("recPhone", jsonArray.get("recPhone").toString());				// 收件详细地址
			map.put("typeOfContents", jsonArray.get("typeOfContents").toString());	// 内件类型
			map.put("nameOfCoutents", jsonArray.get("nameOfCoutents").toString());	// 内件品名
			map.put("mailCode", jsonArray.get("mailCode").toString());				// 产品代码
			map.put("recDatetime", jsonArray.get("recDatetime").toString());		// 寄件日期
			map.put("insuranceValue", jsonArray.get("insuranceValue").toString());	// 保价金额

			Put put = new Put(row_key.getBytes());
			Set set = map.entrySet();
			Iterator iterator = set.iterator();

			while (iterator.hasNext()) {
				Map.Entry mapentry = (Map.Entry) iterator.next();
				put.add("info".getBytes(), ((String)mapentry.getKey()).getBytes(), ((String)mapentry.getValue()).getBytes());
			}

			List<Row> bath = new ArrayList<Row>();
			bath.add(put);
			if (bath.size() == 1000) {
				Object[] reluts = new Object[bath.size()];
				try {
					_hTable.batch(bath, reluts);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				bath.clear();
			}
			map.clear();
		} catch (IOException e) {
			e.printStackTrace();
		}

		_collector.ack(tuple);
    }

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext arg1) {
	}
}
