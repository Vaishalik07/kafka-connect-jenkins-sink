# Domo Connector

## Sink Config

``domo.access.token``
  Domo access token.

  * Type: password
  * Importance: high

``domo.batch.size``
  Number of records in an upload unit.

  * Type: int
  * Importance: high

``domo.client.id``
  Client ID registered with DOMO.

  * Type: password
  * Importance: high

``domo.client.secret``
  Client Secret registered with DOMO.

  * Type: password
  * Importance: high

``domo.stream.name``
  Identifier for the Domo stream to be uploaded to.

  * Type: string
  * Importance: high

``domo.commit.interval``
  Execution commit interval to Domo in minutes.

  * Type: int
  * Default: 15
  * Importance: high

# kafka-connect-jenkins-sink
