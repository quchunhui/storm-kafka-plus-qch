package com.dscn.helloworld.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

@SuppressWarnings("serial")
public class WordCountBolt extends BaseRichBolt {
    private OutputCollector _collector;

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    	System.out.println("WordCountBolt prepare.");
    	_collector = collector;
    }

    public void execute(Tuple input) {
    	System.out.println("WordCountBolt execute. [timestamp]=" + System.currentTimeMillis());
        String str = input.getString(0);
        _collector.emit(new Values(str));
    	System.out.println("WordCountBolt emit.");
        _collector.ack(input);
    	System.out.println("WordCountBolt ack.");
    }

    public void cleanup() {
    	System.out.println("WordCountBolt cleanup.");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	System.out.println("WordCountBolt declareOutputFields.");
        declarer.declare(new Fields("word"));
    }
}