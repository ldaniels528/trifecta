Trifecta 
=======

Trifecta (formerly Verify) is a Command Line Interface (CLI) tool that enables users to quickly and easily inspect
and verify Kafka messages, Storm topologies and Zookeeper data.

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
        * <a href="#kafka-search-by-key">Searching By Key</a>
        * <a href="#kafka-advanced-search">Advanced Search</a>
    * <a href="#storm-module">Storm Module</a>     
    * <a href="#zookeeper-module">Zookeeper Module</a>
        * <a href="#zookeeper-list">Navigating directories and keys</a>    
        * <a href="#zookeeper-get-put">Getting and setting key-value pairs</a>

<a name="Motivations"></a>
## Motivations

The motivations behind creating _Trifecta_ are simple; testing, verifying and managing Kafka topics and Zookeeper 
key-value pairs is an arduous task. The goal of this project is to ease the pain of developing applications that 
make use of Kafka/Storm/ZooKeeper-based via a console-based tool using simple Unix-like commands.

## Status

I'm currently using _Trifecta_ as part of my daily development workflow, and the application itself is undergoing heavy 
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

	$ java -jar trifecta.jar <zookeeperHost>

<a name="usage"></a>
### Usage Examples	

_Trifecta_ exposes its commands through modules. At any time to see which modules are available one could issue the `modules` command.

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
    | avcat       core       Displays the contents of a schema variable                                                      |
    | avload      core       Loads an Avro schema into memory                                                                |
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

**NOTE**: Although the commands are shown in lowercase, they are case insensitive.

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
    | kfind       kafka   Finds messages that corresponds to the given criteria and exports them to a topic               |
    | kfindone    kafka   Returns the first message that corresponds to the given criteria                                |
    | kfirst      kafka   Returns the first message for a given topic                                                     |
    | kget        kafka   Retrieves the message at the specified offset for a given topic partition                       |
    | kgetkey     kafka   Retrieves the key of the message at the specified offset for a given topic partition            |
    | kgetminmax  kafka   Retrieves the smallest and largest message sizes for a range of offsets for a given partition   |
    | kgetsize    kafka   Retrieves the size of the message at the specified offset for a given topic partition           |
    | kinbound    kafka   Retrieves a list of topics with new messages (since last query)                                 |
    | klast       kafka   Returns the last message for a given topic                                                      |
    | kls         kafka   Lists all existing topics                                                                       |
    | knext       kafka   Attempts to retrieve the next message                                                           |
    | kprev       kafka   Attempts to retrieve the message at the previous offset                                         |
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
    + ------------------------------------------------------ +
    | topic                         partitions  replicated   |
    + ------------------------------------------------------ +
    | Shocktrade.quotes.csv         5           100%         |
    | shocktrade.quotes.avro        5           100%         |
    | test.Shocktrade.quotes.avro   5           100%         |
    | hft.Shocktrade.quotes.avro    5           100%         |
    | test2.Shocktrade.quotes.avro  5           100%         |
    | test1.Shocktrade.quotes.avro  5           100%         |
    | test3.Shocktrade.quotes.avro  5           100%         |
    + ------------------------------------------------------ +

To see a subset of the topics (matches any topic that starts with the given search term):

    kafka:/> kls shocktrade.quotes.avro
    + ------------------------------------------------ +
    | topic                   partitions  replicated   |
    + ------------------------------------------------ +
    | shocktrade.quotes.avro  5           100%         |
    + ------------------------------------------------ +

To see a detailed list including all partitions, use the `-l` flag:

    kafka:/> kls -l shocktrade.quotes.avro
    + ------------------------------------------------------------------ +
    | topic                   partition  leader       replicas  inSync   |
    + ------------------------------------------------------------------ +
    | shocktrade.quotes.avro  0          dev501:9092  1         1        |
    | shocktrade.quotes.avro  1          dev501:9093  1         1        |
    | shocktrade.quotes.avro  2          dev502:9091  1         1        |
    | shocktrade.quotes.avro  3          dev502:9092  1         1        |
    | shocktrade.quotes.avro  4          dev502:9093  1         1        |
    + ------------------------------------------------------------------ +

To see the statistics for a specific topic, use the `kstats` command:

    kafka:/> kstats Shocktrade.quotes.csv
    + --------------------------------------------------------------------------------- +
    | topic                      partition  startOffset  endOffset  messagesAvailable   |
    + --------------------------------------------------------------------------------- +
    | Shocktrade.quotes.csv      0          5945         10796      4851                |
    | Shocktrade.quotes.csv      1          5160         10547      5387                |
    | Shocktrade.quotes.csv      2          3974         8788       4814                |
    | Shocktrade.quotes.csv      3          3453         7334       3881                |
    | Shocktrade.quotes.csv      4          4364         8276       3912                |
    + --------------------------------------------------------------------------------- +

<a name="kafka-message-cursor"></a>
##### Kafka Navigable Cursor

The Kafka module offers the concept of a navigable cursor. Any command that references a specific message offset
creates a pointer to that offset, called a navigable cursor. Once the cursor has been established, with a single command, 
you can navigate to the first, last, previous, or next message using the `kfirst`, `klast`, `kprev` and `knext` commands
respectively. Consider the following examples:

To retrieve the first message of a topic partition:

    kafka:/> kfirst Shocktrade.quotes.csv 0
    [5945:000] 22.47.44.46.22.2c.31.30.2e.38.31.2c.22.39.2f.31.32.2f.32.30.31.34.22.2c.22 | "GDF",10.81,"9/12/2014"," |
    [5945:025] 34.3a.30.30.70.6d.22.2c.4e.2f.41.2c.4e.2f.41.2c.2d.30.2e.31.30.2c.22.2d.30 | 4:00pm",N/A,N/A,-0.10,"-0 |
    [5945:050] 2e.31.30.20.2d.20.2d.30.2e.39.32.25.22.2c.31.30.2e.39.31.2c.31.30.2e.39.31 | .10 - -0.92%",10.91,10.91 |
    [5945:075] 2c.31.30.2e.38.31.2c.31.30.2e.39.31.2c.31.30.2e.38.30.2c.33.36.35.35.38.2c | ,10.81,10.91,10.80,36558, |
    [5945:100] 4e.2f.41.2c.22.4e.2f.41.22                                                 | N/A,"N/A"                 |    

The previous command resulted in the creation of a navigable cursor (notice below how our prompt has changed). 

    kafka:Shocktrade.quotes.csv/0:5945> _

Let's view the cursor:

    kafka:Shocktrade.quotes.csv/0:5945> kcursor
    + ------------------------------------------------------------------- +
    | topic                      partition  offset  nextOffset  decoder   |
    + ------------------------------------------------------------------- +
    | Shocktrade.quotes.csv      0          5945    5946                  |
    + ------------------------------------------------------------------- +
    
Let's view the next message for this topic partition:
    
    kafka:Shocktrade.quotes.csv/0:5945> knext
    [5946:000] 22.47.44.50.22.2c.31.38.2e.35.31.2c.22.39.2f.31.32.2f.32.30.31.34.22.2c.22 | "GDP",18.51,"9/12/2014"," |
    [5946:025] 34.3a.30.31.70.6d.22.2c.4e.2f.41.2c.4e.2f.41.2c.2d.30.2e.38.39.2c.22.2d.30 | 4:01pm",N/A,N/A,-0.89,"-0 |
    [5946:050] 2e.38.39.20.2d.20.2d.34.2e.35.39.25.22.2c.31.39.2e.34.30.2c.31.39.2e.32.37 | .89 - -4.59%",19.40,19.27 |
    [5946:075] 2c.31.38.2e.35.31.2c.31.39.2e.33.32.2c.31.38.2e.33.30.2c.31.35.31.36.32.32 | ,18.51,19.32,18.30,151622 |
    [5946:100] 30.2c.38.32.32.2e.33.4d.2c.22.4e.2f.41.22                                  | 0,822.3M,"N/A"            |                                                    | M,"N/A"                   |    
    
Let's view the last message for this topic partition: 

    kafka:Shocktrade.quotes.csv/0:5945> klast
    [10796:000] 22.4e.4f.53.50.46.22.2c.30.2e.30.30.2c.22.4e.2f.41.22.2c.22.4e.2f.41.22.2c | "NOSPF",0.00,"N/A","N/A", |
    [10796:025] 4e.2f.41.2c.4e.2f.41.2c.4e.2f.41.2c.22.4e.2f.41.20.2d.20.4e.2f.41.22.2c.4e | N/A,N/A,N/A,"N/A - N/A",N |
    [10796:050] 2f.41.2c.4e.2f.41.2c.30.2e.30.30.2c.4e.2f.41.2c.4e.2f.41.2c.4e.2f.41.2c.4e | /A,N/A,0.00,N/A,N/A,N/A,N |
    [10796:075] 2f.41.2c.22.54.69.63.6b.65.72.20.73.79.6d.62.6f.6c.20.68.61.73.20.63.68.61 | /A,"Ticker symbol has cha |
    [10796:100] 6e.67.65.64.20.74.6f.3a.20.3c.61.20.68.72.65.66.3d.22.2f.71.3f.73.3d.4e.4f | nged to: <a href="/q?s=NO |
    [10796:125] 53.50.46.22.3e.4e.4f.53.50.46.3c.2f.61.3e.22                               | SPF">NOSPF</a>"           |                                          | ,N/A,"N/A"                |

Notice above we didn't have to specify the topic or partition because it's defined in our cursor. 
Let's view the cursor again:

    kafka:Shocktrade.quotes.csv/0:10796> kcursor
    + ------------------------------------------------------------------- +
    | topic                      partition  offset  nextOffset  decoder   |
    + ------------------------------------------------------------------- +
    | Shocktrade.quotes.csv      0          10796   10797                 |
    + ------------------------------------------------------------------- +

Now, let's view the previous record:

    kafka:Shocktrade.quotes.csv/0:10796> kprev
    [10795:000] 22.4d.4c.50.4b.46.22.2c.30.2e.30.30.2c.22.4e.2f.41.22.2c.22.4e.2f.41.22.2c | "MLPKF",0.00,"N/A","N/A", |
    [10795:025] 4e.2f.41.2c.4e.2f.41.2c.4e.2f.41.2c.22.4e.2f.41.20.2d.20.4e.2f.41.22.2c.4e | N/A,N/A,N/A,"N/A - N/A",N |
    [10795:050] 2f.41.2c.4e.2f.41.2c.30.2e.30.30.2c.4e.2f.41.2c.4e.2f.41.2c.4e.2f.41.2c.4e | /A,N/A,0.00,N/A,N/A,N/A,N |
    [10795:075] 2f.41.2c.22.54.69.63.6b.65.72.20.73.79.6d.62.6f.6c.20.68.61.73.20.63.68.61 | /A,"Ticker symbol has cha |
    [10795:100] 6e.67.65.64.20.74.6f.3a.20.3c.61.20.68.72.65.66.3d.22.2f.71.3f.73.3d.4d.4c | nged to: <a href="/q?s=ML |
    [10795:125] 50.4b.46.22.3e.4d.4c.50.4b.46.3c.2f.61.3e.22                               | PKF">MLPKF</a>"           |                                               | N/A,"N/A"                 |

To retrieve the start and end offsets and number of messages available for a topic across any number of partitions:

    kafka:Shocktrade.quotes.csv/0:10795> kstats
    + --------------------------------------------------------------------------------- +
    | topic                      partition  startOffset  endOffset  messagesAvailable   |
    + --------------------------------------------------------------------------------- +
    | Shocktrade.quotes.csv      0          5945         10796      4851                |
    | Shocktrade.quotes.csv      1          5160         10547      5387                |
    | Shocktrade.quotes.csv      2          3974         8788       4814                |
    | Shocktrade.quotes.csv      3          3453         7334       3881                |
    | Shocktrade.quotes.csv      4          4364         8276       3912                |
    + --------------------------------------------------------------------------------- +

**NOTE**: Above `kstats` is equivalent to both `kstats Shocktrade.quotes.csv` and 
`kstats Shocktrade.quotes.csv 0 4`. However, because of the cursor we previously established, those arguments 
could be omitted.

<a name="kafka-consumer-group"></a>
##### Kafka Consumer Groups

To see the current offsets for all consumer group IDs:

    kafka:Shocktrade.quotes.csv/0:10795> kconsumers
    + ------------------------------------------------------------------------------------- +
    | consumerId  topic                      partition  offset  topicOffset  messagesLeft   |
    + ------------------------------------------------------------------------------------- +
    | dev         Shocktrade.quotes.csv      0          5555    10796        5241           |
    | dev         Shocktrade.quotes.csv      1          0       10547        10547          |
    | dev         Shocktrade.quotes.csv      2          0       8788         8788           |
    | dev         Shocktrade.quotes.csv      3          0       7334         7334           |
    | dev         Shocktrade.quotes.csv      4          0       8276         8276           |
    + ------------------------------------------------------------------------------------- +

Let's change the committed offset for the current topic/partition (the one to which our cursor is pointing) to 6000

    kafka:Shocktrade.quotes.csv/0:10795> kcommit dev 6000

Let's re-examine the consumer group IDs:
    
    kafka:Shocktrade.quotes.csv/0:10795> kconsumers
    + ------------------------------------------------------------------------------------- +
    | consumerId  topic                      partition  offset  topicOffset  messagesLeft   |
    + ------------------------------------------------------------------------------------- +
    | dev         Shocktrade.quotes.csv      0          6000    10796        4796           |
    | dev         Shocktrade.quotes.csv      1          0       10547        10547          |
    | dev         Shocktrade.quotes.csv      2          0       8788         8788           |
    | dev         Shocktrade.quotes.csv      3          0       7334         7334           |
    | dev         Shocktrade.quotes.csv      4          0       8276         8276           |
    + ------------------------------------------------------------------------------------- +

Notice that the committed offset, for consumer group _dev_, has been changed to 6000 for partition 0.

Finally, let's use the `kfetch` to retrieve just the offset for the consumer group ID:

   kafka:Shocktrade.quotes.csv/0:10795> kfetch dev
   6000

<a name="kafka-inbound-traffic"></a>
##### Kafka Inbound Traffic

To retrieve the list of topics with new messages (since your last query):

    kafka:/> kinbound
    + ----------------------------------------------------------------------------------------------------------- +
    | topic                        partition  startOffset  endOffset  change  msgsPerSec  lastCheckTime           |
    + ----------------------------------------------------------------------------------------------------------- +
    | Shocktrade.quotes.avro       3          0            657        65      16.3        09/13/14 06:37:03 PDT   |
    | Shocktrade.quotes.avro       0          0            650        64      16.0        09/13/14 06:37:03 PDT   |
    | Shocktrade.quotes.avro       1          0            618        56      14.0        09/13/14 06:37:03 PDT   |
    | Shocktrade.quotes.avro       2          0            618        49      12.3        09/13/14 06:37:03 PDT   |
    | Shocktrade.quotes.avro       4          0            584        40      10.0        09/13/14 06:37:03 PDT   |
    + ----------------------------------------------------------------------------------------------------------- +

Next, we wait a few moments and run the command again:

    kafka:/> kinbound
    + ----------------------------------------------------------------------------------------------------------- +
    | topic                        partition  startOffset  endOffset  change  msgsPerSec  lastCheckTime           |
    + ----------------------------------------------------------------------------------------------------------- +
    | Shocktrade.quotes.avro       1          0            913        295     15.6        09/13/14 06:37:21 PDT   |
    | Shocktrade.quotes.avro       3          0            952        295     15.6        09/13/14 06:37:21 PDT   |
    | Shocktrade.quotes.avro       2          0            881        263     13.9        09/13/14 06:37:21 PDT   |
    | Shocktrade.quotes.avro       4          0            846        262     13.8        09/13/14 06:37:21 PDT   |
    | Shocktrade.quotes.avro       0          0            893        243     12.8        09/13/14 06:37:21 PDT   |
    + ----------------------------------------------------------------------------------------------------------- +

<a name="kafka-avro-module"></a>
##### Kafka &amp; Avro Integration

_Trifecta_ supports Avro integration for Kafka. The next few examples make use of the following Avro schema:

    {
        "type": "record",
        "name": "StockQuote",
        "namespace": "Shocktrade.avro",
        "fields":[
            { "name": "symbol", "type":"string", "doc":"stock symbol" },
            { "name": "lastTrade", "type":["null", "double"], "doc":"last sale price", "default":null },
            { "name": "tradeDate", "type":["null", "long"], "doc":"last sale date", "default":null },
            { "name": "tradeTime", "type":["null", "string"], "doc":"last sale time", "default":null },
            { "name": "ask", "type":["null", "double"], "doc":"ask price", "default":null },
            { "name": "bid", "type":["null", "double"], "doc":"bid price", "default":null },
            { "name": "change", "type":["null", "double"], "doc":"price change", "default":null },
            { "name": "changePct", "type":["null", "double"], "doc":"price change percent", "default":null },
            { "name": "prevClose", "type":["null", "double"], "doc":"previous close price", "default":null },
            { "name": "open", "type":["null", "double"], "doc":"open price", "default":null },
            { "name": "close", "type":["null", "double"], "doc":"close price", "default":null },
            { "name": "high", "type":["null", "double"], "doc":"day's high price", "default":null },
            { "name": "low", "type":["null", "double"], "doc":"day's low price", "default":null },
            { "name": "volume", "type":["null", "long"], "doc":"day's volume", "default":null },
            { "name": "marketCap", "type":["null", "double"], "doc":"market capitalization", "default":null },
            { "name": "errorMessage", "type":["null", "string"], "doc":"error message", "default":null }
        ],
        "doc": "A schema for stock quotes"
    }
      
Let's load the Avro schema into memory as the variable "schema":
 
    kafka:/> avload schema avro/quotes.avsc

Next, let's use the variable (containing the Avro schema) to decode a Kafka message:

    kafka:/> kfirst Shocktrade.quotes.avro 0 -a schema
    + ------------------------------------- +
    | field         value          type     |
    + ------------------------------------- +
    | symbol        M              Utf8     |
    | lastTrade     59.59          Double   |
    | tradeDate     1410505200000  Long     |
    | tradeTime                             |
    | ask                                   |
    | bid                                   |
    | change        -0.23          Double   |
    | changePct     -0.38          Double   |
    | prevClose     59.82          Double   |
    | open          59.91          Double   |
    | close         59.59          Double   |
    | high          60.06          Double   |
    | low           59.27          Double   |
    | volume        3811771        Long     |
    | marketCap     2.1043E10      Double   |
    | errorMessage                          |
    + ------------------------------------- +

Let's view the cursor:
    
    kafka:Shocktrade.quotes.avro/0:0> kcursor
    + ---------------------------------------------------------------------------------- +
    | topic                         partition  offset  nextOffset  decoder               |
    + ---------------------------------------------------------------------------------- +
    | Shocktrade.quotes.avro        0          0       1           AvroDecoder(schema)   |
    + ---------------------------------------------------------------------------------- +

The `kfirst`, `klast`, `kprev` and `knext` commands also work with the Avro integration:

    kafka:Shocktrade.quotes.avro/0:0> knext
    + ------------------------------------- +
    | field         value          type     |
    + ------------------------------------- +
    | symbol        FNF            Utf8     |
    | lastTrade     27.75          Double   |
    | tradeDate     1410505200000  Long     |
    | tradeTime                             |
    | ask                                   |
    | bid                                   |
    | change        -0.24          Double   |
    | changePct     -0.86          Double   |
    | prevClose     27.99          Double   |
    | open          28.13          Double   |
    | close         27.75          Double   |
    | high          28.23          Double   |
    | low           27.71          Double   |
    | volume        1157144        Long     |
    | marketCap     7.7E9          Double   |
    | errorMessage                          |
    + ------------------------------------- +

<a name="kafka-search-by-key"></a>
##### Kafka Search by Key

You can view the key for any message by using the `kgetkey` command. Let's start by retrieving the last available message 
for a topic/partition.

    kafka:/> klast Shocktrade.quotes.csv 0 
    [10796:000] 22.4e.4f.53.50.46.22.2c.30.2e.30.30.2c.22.4e.2f.41.22.2c.22.4e.2f.41.22.2c | "NOSPF",0.00,"N/A","N/A", |
    [10796:025] 4e.2f.41.2c.4e.2f.41.2c.4e.2f.41.2c.22.4e.2f.41.20.2d.20.4e.2f.41.22.2c.4e | N/A,N/A,N/A,"N/A - N/A",N |
    [10796:050] 2f.41.2c.4e.2f.41.2c.30.2e.30.30.2c.4e.2f.41.2c.4e.2f.41.2c.4e.2f.41.2c.4e | /A,N/A,0.00,N/A,N/A,N/A,N |
    [10796:075] 2f.41.2c.22.54.69.63.6b.65.72.20.73.79.6d.62.6f.6c.20.68.61.73.20.63.68.61 | /A,"Ticker symbol has cha |
    [10796:100] 6e.67.65.64.20.74.6f.3a.20.3c.61.20.68.72.65.66.3d.22.2f.71.3f.73.3d.4e.4f | nged to: <a href="/q?s=NO |
    [10796:125] 53.50.46.22.3e.4e.4f.53.50.46.3c.2f.61.3e.22                               | SPF">NOSPF</a>"           |
    
Now let's view the key for message using the `kgetkey` command:    
    
    kafka:Shocktrade.quotes.csv/0:10796> kgetkey
    [000] 31.34.31.30.35.36.33.37.31.34.34.36.35                                     | 1410563714465             |
    
And for the purposes of fully understanding what happened here, let's reposition the cursor to the beginning of the
topic/partition:

    kafka:Shocktrade.quotes.csv/0:10796> kfirst
    [5945:000] 22.47.44.46.22.2c.31.30.2e.38.31.2c.22.39.2f.31.32.2f.32.30.31.34.22.2c.22 | "GDF",10.81,"9/12/2014"," |
    [5945:025] 34.3a.30.30.70.6d.22.2c.4e.2f.41.2c.4e.2f.41.2c.2d.30.2e.31.30.2c.22.2d.30 | 4:00pm",N/A,N/A,-0.10,"-0 |
    [5945:050] 2e.31.30.20.2d.20.2d.30.2e.39.32.25.22.2c.31.30.2e.39.31.2c.31.30.2e.39.31 | .10 - -0.92%",10.91,10.91 |
    [5945:075] 2c.31.30.2e.38.31.2c.31.30.2e.39.31.2c.31.30.2e.38.30.2c.33.36.35.35.38.2c | ,10.81,10.91,10.80,36558, |
    [5945:100] 4e.2f.41.2c.22.4e.2f.41.22                                                 | N/A,"N/A"                 |
    
    kafka:Shocktrade.quotes.csv/0:5945>     

Next, using the `kfindone`command let's search for the last message by key (using hexadecimal dot-notation):

    kafka:Shocktrade.quotes.csv/0:5945> kfindone key is 31.34.31.30.35.36.33.37.31.34.34.36.35
    Task is now running in the background (use 'jobs' to view)

You may have received a message indicating the the task is now running in the background. This happens when a query is
executed that requires more than 30 seconds to complete. After a few moments, the results should appear on the console:

    kafka:Shocktrade.quotes.csv/0:5945> Job #1006 completed
    [10796:000] 22.4e.4f.53.50.46.22.2c.30.2e.30.30.2c.22.4e.2f.41.22.2c.22.4e.2f.41.22.2c | "NOSPF",0.00,"N/A","N/A", |
    [10796:025] 4e.2f.41.2c.4e.2f.41.2c.4e.2f.41.2c.22.4e.2f.41.20.2d.20.4e.2f.41.22.2c.4e | N/A,N/A,N/A,"N/A - N/A",N |
    [10796:050] 2f.41.2c.4e.2f.41.2c.30.2e.30.30.2c.4e.2f.41.2c.4e.2f.41.2c.4e.2f.41.2c.4e | /A,N/A,0.00,N/A,N/A,N/A,N |
    [10796:075] 2f.41.2c.22.54.69.63.6b.65.72.20.73.79.6d.62.6f.6c.20.68.61.73.20.63.68.61 | /A,"Ticker symbol has cha |
    [10796:100] 6e.67.65.64.20.74.6f.3a.20.3c.61.20.68.72.65.66.3d.22.2f.71.3f.73.3d.4e.4f | nged to: <a href="/q?s=NO |
    [10796:125] 53.50.46.22.3e.4e.4f.53.50.46.3c.2f.61.3e.22                               | SPF">NOSPF</a>"           |

The result is the last message of the topic. Notice that our cursor has changed reflecting the move 
from offset 5945 to 10796.  

<a name="kafka-advanced-search"></a>
##### Kafka Advanced Search

Building on the <a href="#kafka-avro-module">Avro Integration</a>, _Trifecta_ offers the ability to execute queries against 
structured data.

Suppose you want to know how many messages contain a volume greater than 1,000,000, you could issue the `kcount` command:

    kafka:Shocktrade.quotes.avro/0:1> kcount volume > 1000000
    1350

The response was 1350, meaning there are 1350 messages containing a volume greater than 1,000,000.

Suppose you want to find a message for Apple (ticker: "AAPL"), you could issue the `kfindone` command:

    kafka:Shocktrade.quotes.avro/0:1> kfindone symbol == AAPL
    + ------------------------------------- +
    | field         value          type     |
    + ------------------------------------- +
    | symbol        AAPL           Utf8     |
    | lastTrade     101.58         Double   |
    | tradeDate     1410937200000  Long     |
    | tradeTime                             |
    | ask           101.64         Double   |
    | bid           101.5          Double   |
    | change        0.72           Double   |
    | changePct     0.71           Double   |
    | prevClose     100.86         Double   |
    | open          101.32         Double   |
    | close         101.58         Double   |
    | high          101.8          Double   |
    | low           100.5922       Double   |
    | volume        60926496       Long     |
    | marketCap     6.082E11       Double   |
    | errorMessage                          |
    + ------------------------------------- +

Now suppose you want to copy the messages having high volume (1,000,000 or more) to another topic:

    kfind volume >= 1000000 -o hft.Shocktrade.quotes.avro

Finally, let's look at the results:

    kafka:Shocktrade.quotes.avro/3:0> kstats hft.Shocktrade.quotes.avro
    + ---------------------------------------------------------------------------------- +
    | topic                       partition  startOffset  endOffset  messagesAvailable   |
    + ---------------------------------------------------------------------------------- +
    | hft.Shocktrade.quotes.avro  0          0            283        283                 |
    | hft.Shocktrade.quotes.avro  1          0            287        287                 |
    | hft.Shocktrade.quotes.avro  2          0            258        258                 |
    | hft.Shocktrade.quotes.avro  3          0            245        245                 |
    | hft.Shocktrade.quotes.avro  4          0            272        272                 |
    + ---------------------------------------------------------------------------------- +

Let's see how these statistics compares to the original:

    kafka:Shocktrade.quotes.avro/3:0> kstats Shocktrade.quotes.avro
    + ----------------------------------------------------------------------------------- +
    | topic                        partition  startOffset  endOffset  messagesAvailable   |
    + ----------------------------------------------------------------------------------- +
    | Shocktrade.quotes.avro       0          0            4539       4539                |
    | Shocktrade.quotes.avro       1          0            4713       4713                |
    | Shocktrade.quotes.avro       2          0            4500       4500                |
    | Shocktrade.quotes.avro       3          0            4670       4670                |
    | Shocktrade.quotes.avro       4          0            4431       4431                |
    + ----------------------------------------------------------------------------------- +   
            
<a name="storm-module"></a>
#### Storm Module

To view all of the Storm commands, which all begin with the letter "s":

    storm:localhost> ?s
    + -------------------------------------------------------------------------------------- +
    | command   module  description                                                          |
    + -------------------------------------------------------------------------------------- +
    | sbolts    storm   Retrieves the list of bolts for s given topology by ID               |
    | sconf     storm   Lists, retrieves or sets the configuration keys                      |
    | sconnect  storm   Establishes (or re-establishes) a connect to the Storm Nimbus Host   |
    | sget      storm   Retrieves the information for a topology                             |
    | skill     storm   Kills a running topology                                             |
    | sls       storm   Lists available topologies                                           |
    | spouts    storm   Retrieves the list of spouts for a given topology by ID              |
    + -------------------------------------------------------------------------------------- +

Let's view the currently running topologies:

    storm:localhost> sls
    + ---------------------------------------------------------------------------------------------------------------------------------------------- +
    | name                                     topologyId                                            status  workers  executors  tasks  uptimeSecs   |
    + ---------------------------------------------------------------------------------------------------------------------------------------------- +
    | AvroSummaryMetricCounterTopology         AvroSummaryMetricCounterTopology-42-1410749701        ACTIVE  4        48         48     93153        |
    | Hydra-Listener-Traffic-Rates             Hydra-Listener-Traffic-Rates-3-1409671097             ACTIVE  4        30         30     1171757      |
    | TopTalkersTopologyV5                     TopTalkersTopologyV5-44-1410803756                    ACTIVE  4        53         53     39098        |
    | NetworkMonitoringTrafficRateAggregation  NetworkMonitoringTrafficRateAggregation-2-1409671007  ACTIVE  4        22         22     1171847      |
    + ---------------------------------------------------------------------------------------------------------------------------------------------- +

Next, let's look at the details of one of the topologies by ID:

    storm:localhost> sget TopTalkersTopologyV5-44-1410803756
    + --------------------------------------------------- +
    | topologyId                          bolts  spouts   |
    + --------------------------------------------------- +
    | TopTalkersTopologyV5-44-1410803756  7      1        |
    + --------------------------------------------------- +

Let's look at the Topology's bolts:

    storm:localhost> sbolts TopTalkersTopologyV5-44-1410803756
    + ------------------------------------------------------------------------------------------------------------------------------------------- +
    | topologyId                          name                  parallelism  input                 groupingFields  groupingType   tickTupleFreq   |
    + ------------------------------------------------------------------------------------------------------------------------------------------- +
    | TopTalkersTopologyV5-44-1410803756  decoderBolt           10           kafkaSpout                            Local/Shuffle                  |
    | TopTalkersTopologyV5-44-1410803756  kafkaSinkVipOnlyBolt  1            summaryByVipOnlyBolt                  Local/Shuffle                  |
    | TopTalkersTopologyV5-44-1410803756  kafkaSinkVipSiteBolt  1            summaryByVipSiteBolt                  Local/Shuffle                  |
    | TopTalkersTopologyV5-44-1410803756  summaryByVipOnlyBolt  20           decoderBolt           vip             Fields         60              |
    | TopTalkersTopologyV5-44-1410803756  summaryByVipSiteBolt  20           decoderBolt           vip, site       Fields         60              |
    + ------------------------------------------------------------------------------------------------------------------------------------------- +

Let's look at the Topology's spouts:

    storm:localhost> spouts TopTalkersTopologyV5-44-1410803756
    + ------------------------------------------------------------- +
    | topologyId                          name        parallelism   |
    + ------------------------------------------------------------- +
    | TopTalkersTopologyV5-44-1410803756  kafkaSpout  1             |
    + ------------------------------------------------------------- +

Finally, let's take a look at the connection properties for this session:

    storm:localhost> sconf
    + ---------------------------------------------------------------------------------------------------------- +
    | key                                            value                                                       |
    + ---------------------------------------------------------------------------------------------------------- +
    | nimbus.childopts                               -Xmx1024m                                                   |
    | nimbus.host                                    localhost                                                   |
    | nimbus.inbox.jar.expiration.secs               3600                                                        |
    .                                                                                                            .
    .                                                                                                            .
    | nimbus.topology.validator                      backtype.storm.nimbus.DefaultTopologyValidator              |
    | storm.cluster.mode                             distributed                                                 |
    | storm.local.dir                                storm-local                                                 |
    .                                                                                                            .
    .                                                                                                            .
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
    | zcd         zookeeper  Changes the current path/directory in ZooKeeper                    |
    | zexists     zookeeper  Verifies the existence of a ZooKeeper key                          |
    | zget        zookeeper  Retrieves the contents of a specific Zookeeper key                 |
    | zls         zookeeper  Retrieves the child nodes for a key from ZooKeeper                 |
    | zmk         zookeeper  Creates a new ZooKeeper sub-directory (key)                        |
    | zput        zookeeper  Sets a key-value pair in ZooKeeper                                 |
    | zreconnect  zookeeper  Re-establishes the connection to Zookeeper                         |
    | zrm         zookeeper  Removes a key-value from ZooKeeper (DESTRUCTIVE)                   |
    | zruok       zookeeper  Checks the status of a Zookeeper instance (requires netcat)        |
    | zsess       zookeeper  Retrieves the Session ID from ZooKeeper                            |
    | zstat       zookeeper  Returns the statistics of a Zookeeper instance (requires netcat)   |
    | ztree       zookeeper  Retrieves Zookeeper directory structure                            |
    + ----------------------------------------------------------------------------------------- +

<a name="zookeeper-list"></a>    
##### Zookeeper: Navigating directories and keys 

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
    /brokers/topics/shocktrade.quotes.avro
    /brokers/topics/shocktrade.quotes.avro/partitions
    /brokers/topics/shocktrade.quotes.avro/partitions/0
    /brokers/topics/shocktrade.quotes.avro/partitions/0/state
    /brokers/topics/shocktrade.quotes.avro/partitions/1
    /brokers/topics/shocktrade.quotes.avro/partitions/1/state    
    /brokers/topics/shocktrade.quotes.avro/partitions/2
    /brokers/topics/shocktrade.quotes.avro/partitions/2/state    
    /brokers/topics/shocktrade.quotes.avro/partitions/3
    /brokers/topics/shocktrade.quotes.avro/partitions/3/state    
    /brokers/topics/shocktrade.quotes.avro/partitions/4
    /brokers/topics/shocktrade.quotes.avro/partitions/4/state
    /brokers/ids
    /brokers/ids/3
    /brokers/ids/2
    /brokers/ids/1
    /brokers/ids/6
    /brokers/ids/5
    /brokers/ids/4        
    
<a name="zookeeper-get-put"></a>    
##### Zookeeper: Getting and setting key-value pairs        
        
Let's view the contents of one of the keys:        
        
    zookeeper:localhost:2181/brokers> zget topics/Shocktrade.quotes.csv/partitions/4/state
    [00] 7b.22.63.6f.6e.74.72.6f.6c.6c.65.72.5f.65.70.6f.63.68.22.3a.31.2c.22.6c.65 | {"controller_epoch":1,"le
    [25] 61.64.65.72.22.3a.35.2c.22.76.65.72.73.69.6f.6e.22.3a.31.2c.22.6c.65.61.64 | ader":5,"version":1,"lead
    [50] 65.72.5f.65.70.6f.63.68.22.3a.30.2c.22.69.73.72.22.3a.5b.35.5d.7d          | er_epoch":0,"isr":[5]}         

Since we now know the contents of the key is text-based (JSON in this case), let's look at the JSON value using
the type flag (`-t json`) to transform the JSON in a nice human-readable format.

    zookeeper:localhost:2181/brokers> zget topics/Shocktrade.quotes.csv/partitions/4/state -t json
    {
      "controller_epoch": 3,
      "leader": 6,
      "version": 1,
      "leader_epoch": 2,
      "isr": [6]
    }

Next, let's set a key-value pair in Zookeeper:

    zookeeper:localhost:2181/> zput /test/message "Hello World"
   
To retrieve the value we've just set, we can use the `zget` command again:    
    
    zookeeper:localhost:2181/> zget /test/message
    [00] 48.65.6c.6c.6f.20.57.6f.72.6c.64                                           | Hello World    
    
The `zput` command also allows us to set other types of values besides strings. The following example demonstrates 
setting a binary array literal using hexadecimal dot-notation.

    zookeeper:localhost:2181/> zput /test/data de.ad.be.ef.ca.fe.ba.be
    
The default value types for integer and decimal values are `long` and `double` respectively. However, you can also 
explicitly set the value type using the type flag (`-t`).

    zookeeper:localhost:2181/> zput /test/data2 123.45 -t float
    
The valid value types are:

* bytes
* char
* double
* float
* integer
* json
* long
* short
* string

To verify that all of the key-value pairs were inserted we use the `zls` command again:

    zookeeper:localhost:2181/> zls /test
    data
    message
    data2
