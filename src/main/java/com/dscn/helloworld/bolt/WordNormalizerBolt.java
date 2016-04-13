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

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    	System.out.println("WordNormalizerBolt prepare.");
        _collector = collector;
    }

    public void execute(Tuple input) {
    	System.out.println("WordNormalizerBolt execute.");
        String sentence = input.getString(0);
    	System.out.println("WordNormalizerBolt sentence=" + sentence);
        _collector.emit(new Values(sentence));
    	System.out.println("WordNormalizerBolt emit.");
        _collector.ack(input);
    	System.out.println("WordNormalizerBolt ack.");
    }

    public void cleanup() {
    	System.out.println("WordNormalizerBolt cleanup.");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	System.out.println("WordNormalizerBolt declareOutputFields.");
        declarer.declare(new Fields("word"));
    }
}