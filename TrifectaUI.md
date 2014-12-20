Trifecta UI
=======

Trifecta offers a single-page web application (via Angular.js) with a REST service layer and web-socket support,
which offers a comprehensive and powerful set of features for inspecting Kafka topic partitions and messages.

Table of Contents

* <a href="#trifecta-ui">Trifecta UI</a>
    * <a href="#trifecta-ui-start">Starting the embedded web server</a>
    * <a href="#trifecta-ui-configure">Configuring Trifecta UI</a>
    * <a href="#trifecta-ui-decoders">Default Decoders</a>
    * <a href="#trifecta-ui-inspect">Inspecting Kafka Messages</a>
    * <a href="#trifecta-ui-query">Queries</a>

![](http://ldaniels528.github.io/trifecta/images/screenshots/trifecta_ui-observe.png)

<a name="trifecta-ui-start"></a>
#### Starting the embedded web server

To start the embedded web server, issue the following from the command line:

    $ java -jar trifecta.jar --http-start

You'll see a few seconds of log messages, then a prompt indicating the web interface is ready for use.

    Open your browser and navigate to http://localhost:8888

<a name="trifecta-ui-configure"></a>
#### Configuring Trifecta UI

Additionally, Trifecta UI introduces a few new properties to the application configuration file (located in
$HOME/.trifecta/config.properties). **NOTE**: The property values shown below are the default values.

    # the embedded web server host/IP and port for client connections
    trifecta.web.host = localhost
    trifecta.web.port = 8888

    # the interval (in seconds) that changes to consumer offsets will be pushed to web-socket clients
    trifecta.web.push.interval.consumer = 15

    # the interval (in seconds) that changes to topics (new messages) will be pushed to web-socket clients
    trifecta.web.push.interval.topic = 15

    # the number of actors to create for servicing requests
    trifecta.web.actor.concurrency = 10

<a name="trifecta-ui-decoders"></a>
#### Configuring default Avro Decoders

Trifecta UI supports decoding Avro-encoded messages and displaying them in JSON format. To associate an Avro schema to a
Kafka topic, place the schema file in a subdirectory with the same name as the topic. For example, if I wanted to
associate an Avro file named `quotes.avsc` to the Kafka topic `shocktrade.quotes.avro`, I'd setup the following
file structure:

    $HOME/.trifecta/decoders/shocktrade.quotes.avro/quotes.avsc

The name of the actual schema file can be anything you'd like. Once the file has been placed in the appropriate location,
restart Trifecta UI, and your messages will be displayed in JSON format.

![](http://ldaniels528.github.io/trifecta/images/screenshots/trifecta_ui-decoders.png)

<a name="trifecta-ui-inspect"></a>
#### Inspecting Kafka Messages

Trifecta UI has powerful support for viewing Kafka messages, and when the messages are in either JSON or Avro format
Trifecta displays them as human readable (read: pretty) JSON documents.

![](http://ldaniels528.github.io/trifecta/images/screenshots/trifecta_ui-inspect.png)

<a name="trifecta-ui-query"></a>
#### Queries

Trifecta UI also provides a way to execute queries against Avro-encoded topics using the Kafka Query Language (KQL).
KQL is a SQL-like language with syntax as follows:

    select <fields> from <topic> with <decoder>
    [where <condition>]
    [limit <count>]

Consider the following example:

    select symbol, exchange, lastTrade, open, close, high, low
    from "shocktrade.quotes.avro" with default
    where lastTrade <= 1 and volume >= 1,000,000
    limit 25

The above query retrieves the `symbol`, `exchange`, `lastTrade`, `open`, `close`, `high` and `low` fields from messages
within the Kafka topic `shocktrade.quotes.avro` using the `default` decoder filtering for only messages where the
`lastTrade` is less than or equal to `1` and the `volume` is greater than or equal to `1,000,000`, and limiting the
number of results to `25`.

![](http://ldaniels528.github.io/trifecta/images/screenshots/trifecta_ui-query.png)
