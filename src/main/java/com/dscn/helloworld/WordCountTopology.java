package com.dscn.helloworld;

import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import com.dscn.helloworld.bolt.PrintBolt;
import com.dscn.helloworld.bolt.WordCountBolt;
import com.dscn.helloworld.bolt.WordNormalizerBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class WordCountTopology {
    public static void main(String[] args) throws InterruptedException {
    	System.out.println("WordCountTopology main start!");

		BrokerHosts brokerHosts = new ZkHosts("192.168.93.128:2181");
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "qchlocaltest", "/storm", "topo");

		TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RandomSentence", new KafkaSpout(spoutConfig));
        builder.setBolt("WordNormalizer", new WordNormalizerBolt()).shuffleGrouping("RandomSentence");
        builder.setBolt("WordCount", new WordCountBolt()).fieldsGrouping("WordNormalizer", new Fields("word"));
        builder.setBolt("Print", new PrintBolt()).shuffleGrouping("WordCount");

        Config config = new Config();
        config.setDebug(false);

        if (args != null && args.length > 0) {
        	System.out.println("WordCountTopology not local.");
	        try {
	            config.setNumWorkers(1);
	            StormSubmitter.submitTopology("WordCountTopology", config, builder.createTopology());
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
        } else {
        	System.out.println("WordCountTopology local.");
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("WordCountTopology", config, builder.createTopology());
	        Thread.sleep(60000);
	        cluster.shutdown();
        }

    	System.out.println("WordCountTopology main end!");
    }
}
