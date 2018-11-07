package com.vaishali.kafka.connect;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JenkinsSinkTask extends SinkTask {
  private static Logger log = LoggerFactory.getLogger(JenkinsSinkTask.class);
  private JenkinsBuilder jenkinsBuilder;
  private JenkinsSinkConnectorConfig config;

  public JenkinsSinkTask() {
    // no-arg constructor required by Connect framework.
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
      log.info("Starting Jenkins sink task.");
      config = new JenkinsSinkConnectorConfig(props);
      jenkinsBuilder = new JenkinsBuilder(config, context);
  }



  @Override
  public void put(Collection<SinkRecord> records) {
    try {
      jenkinsBuilder.write(records);
    } catch (RetriableException e) {
      jenkinsBuilder.stop();
      throw new RetriableException(e);
    } catch (ConnectException e) {
      jenkinsBuilder.stop();
      throw new ConnectException(e);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    //NOP. The connector is managing the offsets.
  }

  @Override
  public void stop() {
    if (jenkinsBuilder != null) {
      jenkinsBuilder.stop();
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    log.info("In preCommit.");
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    Map<TopicPartition, Long> lastDomoCommitOffsets = jenkinsBuilder.getOffsetsToCommitAndReset();

    if (lastDomoCommitOffsets == null)
        return offsetsToCommit;

    log.info("Committing offsets for : {} partitions", lastDomoCommitOffsets.entrySet().size());
    for (TopicPartition tp: lastDomoCommitOffsets.keySet()) {

      Long offset = lastDomoCommitOffsets.get(tp);
      log.trace("For topic-partition: {},  JenkinsBuilder offset: {}. ",  tp, offset);
      if (offset != null) {
        log.info("Forwarding to framework request to commit offset: {} for {}", offset, tp);
        offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
      }
    }
    return offsetsToCommit;
  }

}
