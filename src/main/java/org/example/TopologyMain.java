package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyMain {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter" , new WordCounter(),2).fieldsGrouping("word-normalizer",new Fields("word"));

        Config conf = new Config();
        conf.put("fileToRead" , "/Users/meet.jayeshpopat/Desktop/WordCountTopology/SampleText.txt");
        conf.put("dirToWrite" ,"/Users/meet.jayeshpopat/Desktop/WordCountTopology/" );
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Word-Counter Topology", conf, builder.createTopology());
            Thread.sleep(30000);

        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            cluster.shutdown();
        }



    }
}
