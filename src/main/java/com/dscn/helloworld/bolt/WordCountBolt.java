package com.dscn.helloworld.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("serial")
public class WordCountBolt extends BaseRichBolt {

    Map<String, Integer> counters;
    private OutputCollector outputCollector;

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    	System.out.println("WordCountBolt prepare.");
        outputCollector = collector;
        counters = new HashMap<String, Integer>();
    }

    public void execute(Tuple input) {
    	System.out.println("WordCountBolt execute.");
        String str = input.getString(0);
        outputCollector.emit(new Values(str));
    	System.out.println("WordCountBolt emit.");
    }

    public void cleanup() {
    	System.out.println("WordCountBolt cleanup.");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	System.out.println("WordCountBolt declareOutputFields.");
        declarer.declare(new Fields("word"));
    }
}