package com.vaishali.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JenkinsSinkConnector extends SinkConnector {
  private static Logger log = LoggerFactory.getLogger(JenkinsSinkConnector.class);
  private Map<String, String> configProperties;
  private JenkinsSinkConnectorConfig config;


  public JenkinsSinkConnector() {
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    try {
      configProperties = props;
      config = new JenkinsSinkConnectorConfig(props);
      log.info("Starting Domo Connector {}", config.getName());
    } catch (ConfigException e) {
        throw new ConnectException("Couldn't start JenkinsSinkConnector due to configuration error", e);
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return JenkinsSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    Map<String, String> taskProps = new HashMap<>();
    taskProps.putAll(configProperties);
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return JenkinsSinkConnectorConfig.conf();
  }
}
