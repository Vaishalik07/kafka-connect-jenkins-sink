package com.vaishali.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;


public class JenkinsSinkConnectorConfig extends AbstractConfig {

  public static final String CLIENT_ID_CONFIG = "domo.client.id";
  private static final String CLIENT_ID_DOC = "Client ID registered with DOMO.";

  public static final String CLIENT_SECRET_CONFIG = "domo.client.secret";
  private static final String CLIENT_SECRET_DOC = "Client Secret registered with DOMO.";

  public static final String STREAM_NAME_CONFIG = "domo.stream.name";
  private static final String STREAM_NAME_DOC = "Identifier for the Domo stream to be uploaded to.";

  public static final String BATCH_SIZE_CONFIG = "domo.batch.size";
  private static final String BATCH_SIZE_DOC = "Number of records in an upload unit.";

  public static final String COMMIT_INTERVAL_CONFIG = "domo.commit.interval";
  private static final String COMMIT_INTERVAL_DOC = "Execution commit interval to Domo in minutes.";
  public static final int COMMIT_INTERVAL_DEFAULT = 15;

  public static final String ACCESS_TOKEN_CONFIG = "domo.access.token";
  private static final String ACCESS_TOKEN_DOC = "Domo access token.";

  private final String name;


  public JenkinsSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
    name = parseName(parsedConfig);
  }

  public JenkinsSinkConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static String parseName(Map<String, String> props) {
    String nameProp = props.get("name");
    return nameProp != null ? nameProp : "Domo-sink";
  }

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(CLIENT_ID_CONFIG,
                Type.PASSWORD,
                Importance.HIGH,
                CLIENT_ID_DOC)
        .define(CLIENT_SECRET_CONFIG,
                Type.PASSWORD,
                Importance.HIGH,
                CLIENT_SECRET_DOC)
        .define(STREAM_NAME_CONFIG,
                Type.STRING,
                Importance.HIGH,
                STREAM_NAME_DOC)
        .define(BATCH_SIZE_CONFIG,
                Type.INT,
                Importance.HIGH,
                BATCH_SIZE_DOC)
        .define(COMMIT_INTERVAL_CONFIG,
                Type.INT,
                COMMIT_INTERVAL_DEFAULT,
                Importance.HIGH,
                COMMIT_INTERVAL_DOC)
        .define(ACCESS_TOKEN_CONFIG,
                Type.PASSWORD,
                Importance.HIGH,
                ACCESS_TOKEN_DOC);
  }

  public String getName() {
    return name;
  }

  public String getAccessToken() {
    return this.getPassword(ACCESS_TOKEN_CONFIG).value();
  }

  public Password getClientId() {
    return this.getPassword(CLIENT_ID_CONFIG);
  }

  public Password getClientSecret() {
    return this.getPassword(CLIENT_SECRET_CONFIG);
  }

  public String getStreamName() {
    return this.getString(STREAM_NAME_CONFIG);
  }

  public Integer getBatchSize() {
    return this.getInt(BATCH_SIZE_CONFIG);
  }

  public Integer getCommitInterval() {
    return this.getInt(COMMIT_INTERVAL_CONFIG);
  }


}
