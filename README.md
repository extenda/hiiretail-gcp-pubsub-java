# Extenda Hii Retail GCP PubSub client
A Google Cloud Platform PubSub client implemented for JDK 21+ (Virtual threads).

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=extenda_hiiretail-gcp-pubsub-java&metric=alert_status&token=6a9c99228f7065b95cf4b2a03f1dc5cde2ae2e74)](https://sonarcloud.io/dashboard?id=extenda_hiiretail-gcp-pubsub-java)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=extenda_hiiretail-gcp-pubsub-java&metric=coverage&token=6a9c99228f7065b95cf4b2a03f1dc5cde2ae2e74)](https://sonarcloud.io/dashboard?id=extenda_hiiretail-gcp-pubsub-java)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=extenda_hiiretail-gcp-pubsub-java&metric=code_smells&token=6a9c99228f7065b95cf4b2a03f1dc5cde2ae2e74)](https://sonarcloud.io/dashboard?id=extenda_hiiretail-gcp-pubsub-java)



## :nut_and_bolt: Configuration

The library supports changing these settings, via environmental variables:

* `SERVICE_PROJECT_ID`

  The value of your GCP project id. Using `test-project` if not set.

* `PUBSUB_CLOSE_TIMEOUT_SECONDS`

  The timeout in seconds before forcefully closing the client.

* `PUBSUB_WAIT_PUBLISH_SECONDS`

  The timeout in seconds to wait for publish result before throwing an error.

## :notebook_with_decorative_cover: Usage

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.google.cloud</groupId>
      <artifactId>libraries-bom</artifactId>
      <version>${version.google-cloud}</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>com.retailsvc</groupId>
    <artifactId>hiiretail-gcp-pubsub-java</artifactId>
    <version>x.y.z</version>
  </dependency>
  <dependency>
    <groupId>com.google.cloud</groupId>
    <artifactId>google-cloud-pubsub</artifactId>
    <version>...</version>
  </dependency>
</dependencies>
```

The library uses `SLF4J` as logging API, so make sure you have `log4j[2]` or `logback` or other
compatible implementation on the classpath.

To use the PubSub client, you first need to instantiate a `PubSubClientFactory`.\
The factory can take a custom (Jackson) `ObjectMapper` as parameter, or use its default.
The mapper is used to convert the objects being sent, to suitable JSONs to publish.
Therefore, you will also need to add `jackson-databind` if not already present:

```xml
<dependency>
  <groupId>com.fasterxml.jackson.core</groupId>
  <artifactId>jackson-databind</artifactId>
  <version>...</version>
</dependency>
```

### Cached clients

Any client that is created via the factory is also cached internally by its topic.

## :scroll: Usage

```java
ObjectMapper objectMapper = new ObjectMapper();
PubSubClientFactory factory = new PubSubClientFactory(objectMapper);
PubSubClient pubSubClient = factory.create("example.entities.v1");

String json = "{ ... }"; // or Record class, POJO etc.
Map<String, String> attributes = Map.of("Tenant-Id", "...", "key", "value");
pubSubClient.publish(json, attributes);
```

## :wrench: Local development environment

* JDK 21+
* Python / pre-commit

### Building

```bash
$ mvn clean package
```

```bash
$ mvn verify
```

#### Install and run the pre-commit hooks before you submit code:

```bash
$ pre-commit install -t pre-commit -t commit-msg
```

## :information_desk_person: Contribution

Contributions to the project are welcome, but must adhere to a few guidelines:

 * [Conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) should be followed
 * Install and use a `editorconfig` plugin to use the project supplied settings


