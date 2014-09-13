Verify
=======

Verify is a Command Line Interface (CLI) tool that enables users to quickly and easily inspect, publish 
and verify messages (or data) to Kafka, Storm and Zookeeper.

Table of Contents

* <a href="#Motivations">Motivations</a>
* <a href="#Development">Development</a>
	* <a href="#build-requirements">Build Requirements</a>
	* <a href="#configuring-your-ide">Configuring the project for your IDE</a>
	* <a href="#building-the-code">Building the code</a>
	* <a href="#testing-the-code">Running the tests</a>	
	* <a href="#running-the-app">Running the application</a>
* <a href="#usage">Usage Examples</a>
    * <a href="#kafka-module">Kafka Module</a>  
        * <a href="#kafka-brokers">Kafka Brokers</a> 
        * <a href="#kafka-topics">Kafka Topics</a> 
        * <a href="#kafka-message-cursor">Navigable Cursor</a>
        * <a href="#kafka-consumer-group">Consumer Groups</a>
        * <a href="kafka-inbound-traffic">Inbound Traffic</a>
        * <a href="#kafka-avro-module">Avro Integration</a>
    * <a href="#storm-module">Storm Module</a>     
    * <a href="#zookeeper-module">Zookeeper Module</a>   

<a name="Motivations"></a>
## Motivations

The motivations behind creating _Verify_ are simple; testing, verifying and managing Kafka topics can be an
arduous task. The goal of this project is to ease the pain of developing applications that make use of 
Kafka/Storm/ZooKeeper-based via a console-based tool using simple Unix-like commands.

## Status

I'm currently using _Verify_ as part of my daily development workflow, and the application itself is undergoing heavy 
development as I define (and at times redefine) its API and command sets. As such, new commands will appear, and older 
commands may be merged with a newer command or disappear altogether. I apologize in advance if a command you were 
fond of has been removed, and if there isn't a suitable replacement command, drop me a note, and perhaps I'll re-add 
the unit of functionality. 
 
**NOTE**: There are a set of hidden commands called _undocumented_ commands. These commands are hidden either because 
they are experimental, work-in-progress, or not yet fully implemented, so use them at your own
risk! To retrieve a list of these _undocumented_ commands, use the `undoc` command.

<a name="Development"></a>
## Development

<a name="build-requirements"></a>
### Build Requirements

* [Java SDK 1.7] (http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html)
* [SBT 0.13+] (http://www.scala-sbt.org/download.html)

<a name="configuring-your-ide"></a>
### Configuring the project for your IDE

#### Generating an Eclipse project
    $ sbt eclipse
    
#### Generating an Intellij Idea project
    $ sbt gen-idea

<a name="building-the-code"></a>
### Building the code

    $ sbt clean assembly
    
<a name="testing-the-code"></a>    
### Running the tests

    $ sbt clean test    

<a name="Running-the-app"></a> 
### Run the application

	$ java -jar verify.jar <zookeeperHost>

<a name="usage"></a>
### Usage Examples	

_Verify_ exposes its commands through modules. At any time to see which modules are available one could issue the `modules` command.

    core:/home/ldaniels> modules
    + ------------------------------------------------------------------------------------- +
    | name       className                                                 loaded  active   |
    + ------------------------------------------------------------------------------------- +
    | kafka      com.ldaniels528.verify.modules.kafka.KafkaModule          true    false    |
    | core       com.ldaniels528.verify.modules.core.CoreModule            true    true     |
    | zookeeper  com.ldaniels528.verify.modules.zookeeper.ZookeeperModule  true    false    |
    | storm      com.ldaniels528.verify.modules.storm.StormModule          true    false    |
    + ------------------------------------------------------------------------------------- +
    
To see all available commands, use the `help` command (`?` is a shortcut):

    core:/home/ldaniels> ?
    + ---------------------------------------------------------------------------------------------------------------------- +
    | command     module     description                                                                                     |
    + ---------------------------------------------------------------------------------------------------------------------- +
    | !           core       Executes a previously issued command                                                            |
    | ?           core       Provides the list of available commands                                                         |
    | autoswitch  core       Automatically switches to the module of the most recently executed command                      |
    | avcat       avro       Displays the contents of a schema variable                                                      |
    | avload      avro       Loads an Avro schema into memory                                                                |
    | cat         core       Dumps the contents of the given file                                                            |
    | cd          core       Changes the local file system path/directory                                                    |
    .                                                                                                                        .
    .                                                                                                                        .
    | kbrokers    kafka      Returns a list of the brokers from ZooKeeper                                                    |
    | kcommit     kafka      Commits the offset for a given topic and group                                                  |
    | kconsumers  kafka      Returns a list of the consumers from ZooKeeper                                                  |
    | kcount      kafka      Counts the messages matching a given condition [references cursor]                              |
    | kcursor     kafka      Displays the current message cursor                                                             |
    .                                                                                                                        .
    .                                                                                                                        .                                          
    | zruok       zookeeper  Checks the status of a Zookeeper instance (requires netcat)                                     |
    | zsess       zookeeper  Retrieves the Session ID from ZooKeeper                                                         |
    | zstat       zookeeper  Returns the statistics of a Zookeeper instance (requires netcat)                                |
    | ztree       zookeeper  Retrieves Zookeeper directory structure                                                         |
    + ---------------------------------------------------------------------------------------------------------------------- +

To see the syntax/usage of a command, use the `syntax` command:

    core:/home/ldaniels> syntax kget
    Description: Retrieves the message at the specified offset for a given topic partition
    Usage: kget [-f outputFile] [-d YYYY-MM-DDTHH:MM:SS] [-a avroSchema] [topic] [partition] [offset]

<a name="kafka-module"></a>
#### Kakfa Module

To view all of the Kafka commands, which all begin with the letter "k":
			
    kafka:/> ?k
    + ------------------------------------------------------------------------------------------------------------------- +
    | command     module  description                                                                                     |
    + ------------------------------------------------------------------------------------------------------------------- +
    | kbrokers    kafka   Returns a list of the brokers from ZooKeeper                                                    |
    | kcommit     kafka   Commits the offset for a given topic and group                                                  |
    | kconsumers  kafka   Returns a list of the consumers from ZooKeeper                                                  |
    | kcount      kafka   Counts the messages matching a given condition [references cursor]                              |
    | kcursor     kafka   Displays the current message cursor                                                             |
    | kfetch      kafka   Retrieves the offset for a given topic and group                                                |
    | kfetchsize  kafka   Retrieves or sets the default fetch size for all Kafka queries                                  |
    | kfindone    kafka   Returns the first message that corresponds to the given criteria [references cursor]            |
    | kfirst      kafka   Returns the first message for a given topic                                                     |
    | kget        kafka   Retrieves the message at the specified offset for a given topic partition                       |
    | kgetminmax  kafka   Retrieves the smallest and largest message sizes for a range of offsets for a given partition   |
    | kgetsize    kafka   Retrieves the size of the message at the specified offset for a given topic partition           |
    | kimport     kafka   Imports messages into a new/existing topic                                                      |
    | kinbound    kafka   Retrieves a list of topics with new messages (since last query)                                 |
    | klast       kafka   Returns the last message for a given topic                                                      |
    | kls         kafka   Lists all existing topics                                                                       |
    | knext       kafka   Attempts to retrieve the next message                                                           |
    | kprev       kafka   Attempts to retrieve the message at the previous offset                                         |
    | kreplicas   kafka   Returns a list of replicas for specified topics                                                 |
    | kreset      kafka   Sets a consumer group ID to zero for all partitions                                             |
    | kstats      kafka   Returns the partition details for a given topic                                                 |
    + ------------------------------------------------------------------------------------------------------------------- +

<a name="kafka-brokers"></a>
##### Kafka Brokers

To list the replica brokers that Zookeeper is aware of:

    kafka:/> kbrokers
    + ---------------------------------------------------------- +
    | jmx_port  timestamp                host    version  port   |
    + ---------------------------------------------------------- +
    | 9999      2014-08-23 19:33:01 PDT  dev501  1        9093   |
    | 9999      2014-08-23 18:41:07 PDT  dev501  1        9092   |
    | 9999      2014-08-23 18:41:07 PDT  dev501  1        9091   |
    | 9999      2014-08-23 20:05:17 PDT  dev502  1        9093   |
    | 9999      2014-08-23 20:05:17 PDT  dev502  1        9092   |
    | 9999      2014-08-23 20:05:17 PDT  dev502  1        9091   |
    + ---------------------------------------------------------- +

<a name="kafka-topics"></a>
##### Kafka Topics

To list all of the Kafka topics that Zookeeper is aware of:

    kafka:/> kls
    + ------------------------------------------------------------------- +
    | topic                      partition  leader       replicas  inSync |
    + ------------------------------------------------------------------- +
    | com.shocktrade.quotes.rt   0          dev502:9093  1         1      |
    | com.shocktrade.quotes.rt   1          dev501:9091  1         1      |
    | com.shocktrade.quotes.rt   2          dev501:9092  1         1      |
    | com.shocktrade.quotes.rt   3          dev501:9093  1         1      |
    | com.shocktrade.quotes.rt   4          dev502:9091  1         1      |
    | com.shocktrade.quotes.csv  0          dev501:9091  1         1      |
    | com.shocktrade.quotes.csv  1          dev501:9092  1         1      |
    | com.shocktrade.quotes.csv  2          dev501:9093  1         1      |
    | com.shocktrade.quotes.csv  3          dev502:9091  1         1      |
    | com.shocktrade.quotes.csv  4          dev502:9092  1         1      |
    + ------------------------------------------------------------------- +

To see a subset of the topics (matches any topic that starts with the given search term):

    kafka:/> kls com.shocktrade.quotes.csv
    + ------------------------------------------------------------------- +
    | topic                      partition  leader       replicas  inSync |
    + ------------------------------------------------------------------- +
    | com.shocktrade.quotes.csv  0          dev501:9091  1         1      |
    | com.shocktrade.quotes.csv  1          dev501:9092  1         1      |
    | com.shocktrade.quotes.csv  2          dev501:9093  1         1      |
    | com.shocktrade.quotes.csv  3          dev502:9091  1         1      |
    | com.shocktrade.quotes.csv  4          dev502:9092  1         1      |
    + ------------------------------------------------------------------- +

To see the statistics for a specific topic, use the `kstats` command:

    kafka:/> kstats com.shocktrade.quotes.csv
    + --------------------------------------------------------------------------------- +
    | topic                      partition  startOffset  endOffset  messagesAvailable   |
    + --------------------------------------------------------------------------------- +
    | com.shocktrade.quotes.csv  0          5945         10796      4851                |
    | com.shocktrade.quotes.csv  1          5160         10547      5387                |
    | com.shocktrade.quotes.csv  2          3974         8788       4814                |
    | com.shocktrade.quotes.csv  3          3453         7334       3881                |
    | com.shocktrade.quotes.csv  4          4364         8276       3912                |
    + --------------------------------------------------------------------------------- +

<a name="kafka-message-cursor"></a>
##### Kafka Navigable Cursor

The Kafka module offers the concept of a navigable cursor. Any command that references a specific message offset
creates a pointer to that offset, called a navigable cursor. Once the cursor has been established, with a single command, 
you can navigate to the first, last, previous, or next message using the `kfirst`, `klast`, `kprev` and `knext` commands
respectively. Consider the following examples:

To retrieve the first message of a topic partition:

    kafka:/> kfirst com.shocktrade.quotes.csv 0
    [5945:000] 22.47.44.46.22.2c.31.30.2e.38.31.2c.22.39.2f.31.32.2f.32.30.31.34.22.2c.22 | "GDF",10.81,"9/12/2014"," |
    [5945:025] 34.3a.30.30.70.6d.22.2c.4e.2f.41.2c.4e.2f.41.2c.2d.30.2e.31.30.2c.22.2d.30 | 4:00pm",N/A,N/A,-0.10,"-0 |
    [5945:050] 2e.31.30.20.2d.20.2d.30.2e.39.32.25.22.2c.31.30.2e.39.31.2c.31.30.2e.39.31 | .10 - -0.92%",10.91,10.91 |
    [5945:075] 2c.31.30.2e.38.31.2c.31.30.2e.39.31.2c.31.30.2e.38.30.2c.33.36.35.35.38.2c | ,10.81,10.91,10.80,36558, |
    [5945:100] 4e.2f.41.2c.22.4e.2f.41.22                                                | N/A,"N/A"                 |    

The previous command resulted in the creation of a navigable cursor (notice below how our prompt has changed). 

    kafka:com.shocktrade.quotes.csv/0:5945> _

Let's view the cursor:

    kafka:com.shocktrade.quotes.csv/0:5945> kcursor
    + ------------------------------------------------------------------- +
    | topic                      partition  offset  nextOffset  decoder   |
    + ------------------------------------------------------------------- +
    | com.shocktrade.quotes.csv  0          5945    5946                  |
    + ------------------------------------------------------------------- +
    
Let's view the next message for this topic partition:
    
    kafka:com.shocktrade.quotes.csv/0:5945> knext
    [5946:000] 22.47.44.50.22.2c.31.38.2e.35.31.2c.22.39.2f.31.32.2f.32.30.31.34.22.2c.22 | "GDP",18.51,"9/12/2014"," |
    [5946:025] 34.3a.30.31.70.6d.22.2c.4e.2f.41.2c.4e.2f.41.2c.2d.30.2e.38.39.2c.22.2d.30 | 4:01pm",N/A,N/A,-0.89,"-0 |
    [5946:050] 2e.38.39.20.2d.20.2d.34.2e.35.39.25.22.2c.31.39.2e.34.30.2c.31.39.2e.32.37 | .89 - -4.59%",19.40,19.27 |
    [5946:075] 2c.31.38.2e.35.31.2c.31.39.2e.33.32.2c.31.38.2e.33.30.2c.31.35.31.36.32.32 | ,18.51,19.32,18.30,151622 |
    [5946:100] 30.2c.38.32.32.2e.33.4d.2c.22.4e.2f.41.22                                  | 0,822.3M,"N/A"            |                                                    | M,"N/A"                   |    
    
Let's view the last message for this topic partition: 

    kafka:com.shocktrade.quotes.csv/0:5945> klast
    [10796:000] 22.4e.4f.53.50.46.22.2c.30.2e.30.30.2c.22.4e.2f.41.22.2c.22.4e.2f.41.22.2c | "NOSPF",0.00,"N/A","N/A", |
    [10796:025] 4e.2f.41.2c.4e.2f.41.2c.4e.2f.41.2c.22.4e.2f.41.20.2d.20.4e.2f.41.22.2c.4e | N/A,N/A,N/A,"N/A - N/A",N |
    [10796:050] 2f.41.2c.4e.2f.41.2c.30.2e.30.30.2c.4e.2f.41.2c.4e.2f.41.2c.4e.2f.41.2c.4e | /A,N/A,0.00,N/A,N/A,N/A,N |
    [10796:075] 2f.41.2c.22.54.69.63.6b.65.72.20.73.79.6d.62.6f.6c.20.68.61.73.20.63.68.61 | /A,"Ticker symbol has cha |
    [10796:100] 6e.67.65.64.20.74.6f.3a.20.3c.61.20.68.72.65.66.3d.22.2f.71.3f.73.3d.4e.4f | nged to: <a href="/q?s=NO |
    [10796:125] 53.50.46.22.3e.4e.4f.53.50.46.3c.2f.61.3e.22                               | SPF">NOSPF</a>"           |                                          | ,N/A,"N/A"                |

Notice above we didn't have to specify the topic or partition because it's defined in our cursor. 
Let's view the cursor again:

    kafka:com.shocktrade.quotes.csv/0:10796> kcursor
    + ------------------------------------------------------------------- +
    | topic                      partition  offset  nextOffset  decoder   |
    + ------------------------------------------------------------------- +
    | com.shocktrade.quotes.csv  0          10796   10797                 |
    + ------------------------------------------------------------------- +

Now, let's view the previous record:

    kafka:com.shocktrade.quotes.csv/0:10796> kprev
    [10795:000] 22.4d.4c.50.4b.46.22.2c.30.2e.30.30.2c.22.4e.2f.41.22.2c.22.4e.2f.41.22.2c | "MLPKF",0.00,"N/A","N/A", |
    [10795:025] 4e.2f.41.2c.4e.2f.41.2c.4e.2f.41.2c.22.4e.2f.41.20.2d.20.4e.2f.41.22.2c.4e | N/A,N/A,N/A,"N/A - N/A",N |
    [10795:050] 2f.41.2c.4e.2f.41.2c.30.2e.30.30.2c.4e.2f.41.2c.4e.2f.41.2c.4e.2f.41.2c.4e | /A,N/A,0.00,N/A,N/A,N/A,N |
    [10795:075] 2f.41.2c.22.54.69.63.6b.65.72.20.73.79.6d.62.6f.6c.20.68.61.73.20.63.68.61 | /A,"Ticker symbol has cha |
    [10795:100] 6e.67.65.64.20.74.6f.3a.20.3c.61.20.68.72.65.66.3d.22.2f.71.3f.73.3d.4d.4c | nged to: <a href="/q?s=ML |
    [10795:125] 50.4b.46.22.3e.4d.4c.50.4b.46.3c.2f.61.3e.22                               | PKF">MLPKF</a>"           |                                               | N/A,"N/A"                 |

To retrieve the start and end offsets and number of messages available for a topic across any number of partitions:

    kafka:com.shocktrade.quotes.csv/0:10795> kstats
    + --------------------------------------------------------------------------------- +
    | topic                      partition  startOffset  endOffset  messagesAvailable   |
    + --------------------------------------------------------------------------------- +
    | com.shocktrade.quotes.csv  0          5945         10796      4851                |
    | com.shocktrade.quotes.csv  1          5160         10547      5387                |
    | com.shocktrade.quotes.csv  2          3974         8788       4814                |
    | com.shocktrade.quotes.csv  3          3453         7334       3881                |
    | com.shocktrade.quotes.csv  4          4364         8276       3912                |
    + --------------------------------------------------------------------------------- +

**NOTE**: Above `kstats` is equivalent to both `kstats com.shocktrade.quotes.csv` and 
`kstats com.shocktrade.quotes.csv 0 4`. However, because of the cursor we previously established, those arguments 
could be omitted.

<a name="kafka-consumer-group"></a>
##### Kafka Consumer Groups

To see the current offsets for all consumer group IDs:

    kafka:com.shocktrade.quotes.csv/0:10795> kconsumers
    + ------------------------------------------------------------------------------------- +
    | consumerId  topic                      partition  offset  topicOffset  messagesLeft   |
    + ------------------------------------------------------------------------------------- +
    | dev         com.shocktrade.quotes.csv  0          5555    10796        5241           |
    | dev         com.shocktrade.quotes.csv  1          0       10547        10547          |
    | dev         com.shocktrade.quotes.csv  2          0       8788         8788           |
    | dev         com.shocktrade.quotes.csv  3          0       7334         7334           |
    | dev         com.shocktrade.quotes.csv  4          0       8276         8276           |
    + ------------------------------------------------------------------------------------- +

<a name="kafka-inbound-traffic"></a>
##### Kafka Inbound Traffic

To retrieve the list of topics with new messages (since your last query):

    kafka:com.shocktrade.quotes.csv/0:10795> kinbound
    + --------------------------------------------------------------------------------------------------------- +
    | topic                      partition  startOffset  endOffset  change  msgsPerSec  lastCheckTime           |
    + --------------------------------------------------------------------------------------------------------- +
    | com.shocktrade.quotes.csv  4          0            9138       36      12.0        09/01/14 01:51:30 PDT   |
    | com.shocktrade.quotes.csv  0          0            9624       32      10.7        09/01/14 01:51:30 PDT   |
    | com.shocktrade.quotes.csv  1          0            10492      32      10.7        09/01/14 01:51:30 PDT   |
    | com.shocktrade.quotes.csv  2          0            11018      32      10.7        09/01/14 01:51:30 PDT   |
    | com.shocktrade.quotes.csv  3          0            10031      27      9.0         09/01/14 01:51:30 PDT   |
    + --------------------------------------------------------------------------------------------------------- +

<a name="kafka-avro-module"></a>
##### Kafka &amp; Avro Integration

Verify supports Avro integration for Kafka. The next few examples make use of the following Avro schema:

    {
      "type": "record",
      "name": "TopTalkers",
      "namespace": "com.shocktrade.avro",
      "fields": [
        { "name": "vip", "type": "string", "doc": "The Internally-issued IP address" },
        { "name": "site", "type": "string", "doc": "The top-level-domain/site" },
        { "name": "srcIP", "type": "string", "doc": "The source IP address" },
        { "name": "frequency", "type": "long", "doc": "The number of occurrences of the vip, site and source IP tuple" },
        { "name": "firstTimestamp", "type": "long", "doc": "The first occurrence of the source IP" },
        { "name": "lastTimestamp", "type": "long", "doc": "The last occurrence of the source IP" }
      ],
      "doc": "A basic schema for top-talkers messages"
    }
      

Let's load the Avro schema into memory as the variable "topTalkers":
 
    kafka:com.shocktrade.quotes.csv/0:9580> avload topTalkers avro/topTalkers.avsc

Next, let's use the variable (containing the Avro schema) to decode a Kafka message:

    kafka:com.shocktrade.quotes.csv/0:9580> kgeta topTalkers com.shocktrade.topTalkers  0 0
    + ------------------------------------ +
    | field           value         type   |
    + ------------------------------------ +
    | vip             192.73.24.30  Utf8   |
    | site            vxfjc3        Utf8   |
    | srcIP           68.152.96.28  Utf8   |
    | frequency       1071          Long   |
    | firstTimestamp  1409979852    Long   |
    | lastTimestamp   1409979916    Long   |
    + ------------------------------------ +

Let's view the cursor:
    
    kafka:com.shocktrade.topTalkers/0:0> kcursor
    + --------------------------------------------------------------------------------- +
    | topic                      partition  offset  nextOffset  decoder                 |
    + --------------------------------------------------------------------------------- +
    | com.shocktrade.topTalkers  0          0       1           AvroDecoder(topTalkers) |
    + --------------------------------------------------------------------------------- +  

The `kfirst`, `klast`, `kprev` and `knext` commands also work with the Avro integration:

    kafka:com.shocktrade.topTalkers/0:0> knext
    + ------------------------------------ +
    | field           value         type   |
    + ------------------------------------ +
    | vip             191.73.24.30  Utf8   |
    | site            vxfjc3        Utf8   |
    | srcIP           69.252.96.17  Utf8   |
    | frequency       1085          Long   |
    | firstTimestamp  1409979852    Long   |
    | lastTimestamp   1409979916    Long   |
    + ------------------------------------ +

Suppose you want to know how many messages contain a frequency greater than 2500, you could issue the `kCount` command:

    kafka:com.shocktrade.topTalkers/7:3> kCount frequency > 2500
    106

The response was 106, meaning there are 106 messages containing a frequency greater than 2500.

Now suppose you want to view the first message whose frequency is  greater than 2500, you could issue the `kFindOne` command:

    kafka:com.shocktrade.topTalkers/0:0> kFindOne frequency > 2500
    + ------------------------------------- +
    | field           value          type   |
    + ------------------------------------- +
    | vip             191.78.128.30  Utf8   |
    | site            vxfjc4         Utf8   |
    | srcIP           172.16.14.15   Utf8   |
    | frequency       4150           Long   |
    | firstTimestamp  1410038836     Long   |
    | lastTimestamp   1410038896     Long   |
    + ------------------------------------- +

<a name="storm-module"></a>
#### Storm Module

To view all of the Storm commands, which all begin with the letter "s":

    storm:localhost> ?s
    + --------------------------------------------------------------------------- +
    | command   module  description                                               |
    + --------------------------------------------------------------------------- +
    | sbolts    storm   Retrieves the list of bolts for s given topology by ID    |
    | sconf     storm   Lists, retrieves or sets the configuration keys           |
    | sconnect  storm   Establishes a connect to the Storm Nimbus Host            |
    | sdeploy   storm   Deploys a topology to the Storm server (EXPERIMENTAL)     |
    | sget      storm   Retrieves the information for a topology                  |
    | skill     storm   Kills a running topology                                  |
    | sls       storm   Lists available topologies                                |
    | spouts    storm   Retrieves the list of spouts for a given topology by ID   |
    + --------------------------------------------------------------------------- +

Let's view the currently running topologies:

    storm:localhost> sls
    + ---------------------------------------------------------------------------------------------------------------------------------------------- +
    | name                                     topologyId                                            status  workers  executors  tasks  uptimeSecs   |
    + ---------------------------------------------------------------------------------------------------------------------------------------------- +
    | nm-traffic-rate-aggregation-xxmyller     nm-traffic-rate-aggregation-xxmyller-17-1407973634    ACTIVE  4        22         22     1619957      |
    | CTSC0-Traffic-Categorizer                CTSC0-Traffic-Categorizer-8-1408969694                ACTIVE  4        125        125    623897       |
    | NetworkMonitoringTrafficRateAggregation  NetworkMonitoringTrafficRateAggregation-9-1409160151  ACTIVE  4        22         22     433440       |
    | Hydra-Listener-Traffic-Rates             Hydra-Listener-Traffic-Rates-13-1407867259            ACTIVE  4        30         30     1726332      |
    | nm-traffic-rate-aggregation              nm-traffic-rate-aggregation-10-1407854552             ACTIVE  4        22         22     1739039      |
    + ---------------------------------------------------------------------------------------------------------------------------------------------- +

Next, let's look at the details of one of the topologies by ID:

    storm:localhost> sget nm-traffic-rate-aggregation-xxmyller-17-1407973634
    + ------------------------------------------------------------------- +
    | topologyId                                          bolts  spouts   |
    + ------------------------------------------------------------------- +
    | nm-traffic-rate-aggregation-xxmyller-17-1407973634  5      1        |
    + ------------------------------------------------------------------- +

Let's look at the Topology's bolts:

    zookeeper:vsccrtc201-brn1:2181/> sbolts nm-traffic-rate-aggregation-xxmyller-17-1407973634
    + ------------------------------------------------------------------------------------ +
    | topologyId                                          name                             |
    + ------------------------------------------------------------------------------------ +
    | nm-traffic-rate-aggregation-xxmyller-17-1407973634  nm-aggregation-kafka-sink-bolt   |
    | nm-traffic-rate-aggregation-xxmyller-17-1407973634  nm-aggregation-tuple-bolt        |
    | nm-traffic-rate-aggregation-xxmyller-17-1407973634  __acker                          |
    | nm-traffic-rate-aggregation-xxmyller-17-1407973634  __system                         |
    | nm-traffic-rate-aggregation-xxmyller-17-1407973634  nm-aggregation-reporting-bolt    |
    + ------------------------------------------------------------------------------------ +

Let's look at the Topology's spouts:

    storm:localhost> spouts nm-traffic-rate-aggregation-xxmyller-17-1407973634
    + -------------------------------------------------------------------------- +
    | topologyId                                          name                   |
    + -------------------------------------------------------------------------- +
    | nm-traffic-rate-aggregation-xxmyller-17-1407973634  nm-aggregation-spout   |
    + -------------------------------------------------------------------------- +

Finally, let's take a look at the connection properties for this session:

    storm:localhost> sconf
    + ---------------------------------------------------------------------------------------------------------- +
    | key                                            value                                                       |
    + ---------------------------------------------------------------------------------------------------------- +
    | nimbus.childopts                               -Xmx1024m                                                   |
    | nimbus.cleanup.inbox.freq.secs                 600                                                         |
    | nimbus.file.copy.expiration.secs               600                                                         |
    | nimbus.host                                    localhost                                                   |
    | nimbus.inbox.jar.expiration.secs               3600                                                        |
    | nimbus.monitor.freq.secs                       10                                                          |
    | nimbus.reassign                                true                                                        |
    | nimbus.supervisor.timeout.secs                 60                                                          |
    | nimbus.task.launch.secs                        120                                                         |
    | nimbus.task.timeout.secs                       30                                                          |
    | nimbus.thrift.max_buffer_size                  1048576                                                     |
    | nimbus.thrift.port                             6627                                                        |
    | nimbus.topology.validator                      backtype.storm.nimbus.DefaultTopologyValidator              |
    | storm.cluster.mode                             distributed                                                 |
    | storm.local.dir                                storm-local                                                 |
    | storm.local.mode.zmq                           false                                                       |
    | storm.messaging.netty.buffer_size              5242880                                                     |
    | storm.messaging.netty.client_worker_threads    1                                                           |
    | storm.messaging.netty.flush.check.interval.ms  10                                                          |
    | storm.messaging.netty.max_retries              30                                                          |
    | storm.messaging.netty.max_wait_ms              1000                                                        |
    | storm.messaging.netty.min_wait_ms              100                                                         |
    | storm.messaging.netty.server_worker_threads    1                                                           |
    | storm.messaging.netty.transfer.batch.size      262144                                                      |
    | storm.messaging.transport                      backtype.storm.messaging.netty.Context                      |
    | storm.thrift.transport                         backtype.storm.security.auth.SimpleTransportPlugin          |
    | storm.zookeeper.connection.timeout             15000                                                       |
    | storm.zookeeper.port                           2181                                                        |
    | storm.zookeeper.retry.interval                 1000                                                        |
    | storm.zookeeper.retry.intervalceiling.millis   30000                                                       |
    | storm.zookeeper.retry.times                    5                                                           |
    | storm.zookeeper.root                           /storm                                                      |
    | storm.zookeeper.servers                        [localhost]                                                 |
    | storm.zookeeper.session.timeout                20000                                                       |
    .
    .
    .
    | worker.heartbeat.frequency.secs                1                                                           |
    | zmq.hwm                                        0                                                           |
    | zmq.linger.millis                              5000                                                        |
    | zmq.threads                                    1                                                           |
    + ---------------------------------------------------------------------------------------------------------- +

<a name="zookeeper-module"></a>
#### Zookeeper Module

To view all of the Zookeeper commands, which all begin with the letter "z":

    zookeeper:localhost:2181/> ?z
    + ----------------------------------------------------------------------------------------- +
    | command     module     description                                                        |
    + ----------------------------------------------------------------------------------------- +
    | zcat        zookeeper  Retrieves the value of a key from ZooKeeper                        |
    | zcd         zookeeper  Changes the current path/directory in ZooKeeper                    |
    | zexists     zookeeper  Verifies the existence of a ZooKeeper key                          |
    | zget        zookeeper  Retrieves the contents of a specific Zookeeper key                 |
    | zls         zookeeper  Retrieves the child nodes for a key from ZooKeeper                 |
    | zmk         zookeeper  Creates a new ZooKeeper sub-directory (key)                        |
    | zput        zookeeper  Retrieves a value from ZooKeeper                                   |
    | zreconnect  zookeeper  Re-establishes the connection to Zookeeper                         |
    | zrm         zookeeper  Removes a key-value from ZooKeeper (DESTRUCTIVE)                   |
    | zruok       zookeeper  Checks the status of a Zookeeper instance (requires netcat)        |
    | zsess       zookeeper  Retrieves the Session ID from ZooKeeper                            |
    | zstat       zookeeper  Returns the statistics of a Zookeeper instance (requires netcat)   |
    | ztree       zookeeper  Retrieves Zookeeper directory structure                            |
    + ----------------------------------------------------------------------------------------- +

To view the Zookeeper keys at the current hierarchy level:

	zookeeper@dev501:2181:/> zls
		consumers
		storm
		controller_epoch
		admin
		controller
		brokers	
			
To change the current Zookeeper hierarchy level:			
			
    zookeeper:localhost:2181:/> zcd brokers
        /brokers
        
Now view the keys at this level:        
    
    zookeeper:localhost:2181:/brokers> zls
        topics
        ids	
        
Let's look at the entire Zookeeper hierarchy recursively from our current path:
        
    zookeeper:localhost:2181/brokers> ztree
    /brokers
    /brokers/topics
    /brokers/topics/csvQuotes
    /brokers/topics/csvQuotes/partitions
    /brokers/topics/csvQuotes/partitions/3
    /brokers/topics/csvQuotes/partitions/3/state
    /brokers/topics/csvQuotes/partitions/2
    /brokers/topics/csvQuotes/partitions/2/state
    /brokers/topics/csvQuotes/partitions/1
    /brokers/topics/csvQuotes/partitions/1/state
    /brokers/topics/csvQuotes/partitions/0
    /brokers/topics/csvQuotes/partitions/0/state
    /brokers/topics/csvQuotes/partitions/4
    /brokers/topics/csvQuotes/partitions/4/state
    /brokers/topics/com.shocktrade.quotes.csv
    /brokers/topics/com.shocktrade.quotes.csv/partitions
    /brokers/topics/com.shocktrade.quotes.csv/partitions/3
    /brokers/topics/com.shocktrade.quotes.csv/partitions/3/state
    /brokers/topics/com.shocktrade.quotes.csv/partitions/2
    /brokers/topics/com.shocktrade.quotes.csv/partitions/2/state
    /brokers/topics/com.shocktrade.quotes.csv/partitions/1
    /brokers/topics/com.shocktrade.quotes.csv/partitions/1/state
    /brokers/topics/com.shocktrade.quotes.csv/partitions/0
    /brokers/topics/com.shocktrade.quotes.csv/partitions/0/state
    /brokers/topics/com.shocktrade.quotes.csv/partitions/4
    /brokers/topics/com.shocktrade.quotes.csv/partitions/4/state
    /brokers/ids
    /brokers/ids/3
    /brokers/ids/2
    /brokers/ids/1
    /brokers/ids/6
    /brokers/ids/5
    /brokers/ids/4        
        
Let's view the contents of one of the keys:        
        
    zookeeper:localhost:2181/brokers> zget topics/com.shocktrade.quotes.csv/partitions/4/state
    [00] 7b.22.63.6f.6e.74.72.6f.6c.6c.65.72.5f.65.70.6f.63.68.22.3a.31.2c.22.6c.65 | {"controller_epoch":1,"le
    [25] 61.64.65.72.22.3a.35.2c.22.76.65.72.73.69.6f.6e.22.3a.31.2c.22.6c.65.61.64 | ader":5,"version":1,"lead
    [50] 65.72.5f.65.70.6f.63.68.22.3a.30.2c.22.69.73.72.22.3a.5b.35.5d.7d          | er_epoch":0,"isr":[5]}         

Since we now know the contents of the key is text-based (JSON in this case), let's look at the plain-text value.
**NOTE:** This command comes in handy when you want to copy/paste the value of a key.

    zookeeper:localhost:2181/brokers> zcat topics/com.shocktrade.quotes.csv/partitions/4/state text
    {"controller_epoch":1,"leader":5,"version":1,"leader_epoch":0,"isr":[5]}
