Trifecta UI
=======

Trifecta offers a single-page web application (via Angular.js) with a REST service layer and web-socket support,
which offers many of the powerful features found in the CLI client application.

Table of Contents

* <a href="#trifecta-ui">Trifecta UI</a>
    * <a href="#trifecta-ui-start">Starting the embedded web server</a>
    * <a href="#trifecta-ui-configure">Configuring Trifecta UI</a>
    * <a href="#trifecta-ui-decoders">Default Decoders</a>
    * <a href="#trifecta-ui-query">Queries</a>

![](http://ldaniels528.github.io/trifecta/images/screenshots/trifecta_ui-inspect.png)

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

Additionally, once a "default" decoder is configured for a Kafka topic, the CLI application can use them as well.
For more details about using default decoders with the CLI application <a href="#kafka-default-avro-decoder">click here</a>.

<a name="trifecta-ui-query"></a>
#### Queries

Trifecta UI also provides a way to execute queries against topics using BDQL (Big Data Query Language). For more
detailed information about BDQL queries, <a href="#kafka-search-by-query">click here</a>.

![](http://ldaniels528.github.io/trifecta/images/screenshots/trifecta_ui-query.png)
