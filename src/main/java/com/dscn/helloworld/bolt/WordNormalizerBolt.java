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
public class WordNormalizerBolt extends BaseRichBolt {
    private OutputCollector _collector;
    long startTime = System.currentTimeMillis();
    long count = 0;

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    public void execute(Tuple input) {

    	count++;
        if (count % 200000 == 0) {
        	long sumTime = System.currentTimeMillis();
        	System.out.println("[RESULT]the time of 20000 is [" + (sumTime - startTime) + "]");
        }

    	String sentence = input.getString(0);
        _collector.emit(new Values(sentence));
        _collector.ack(input);
    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}