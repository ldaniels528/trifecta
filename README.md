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

<a name="Motivations"></a>
## Motivations

The motivations behind creating _Verify_ are simple; testing, verifying and managing Kafka topics can be an
arduous task. The goal of this project is to ease the pain of developing applications that make use of 
Kafka/Storm/ZooKeeper-based via a console-based tool using simple Unix-like commands.

<a name="Development"></a>
## Development

<a name="build-requirements"></a>
### Build Requirements

* [Java SDK 1.7] (http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html)
* [Tabular] (https://github.com/ldaniels528/tabular)
* [SBT 0.13+] (http://www.scala-sbt.org/download.html)

<a name="configuring-your-ide"></a>
### Configuring the project for your IDE

#### Generating an Eclipse project
    $ sbt eclipse
    
#### Generating an Intellij Idea project
    $ sbt gen-idea

<a name="building-the-code"></a>
### Building the code

    $ sbt assembly
    
<a name="testing-the-code"></a>    
### Running the tests

    $ sbt test    

<a name="Running-the-app"></a> 
### Run the application

	$ java -jar verify.jar <zookeeperHost>

<a name="usage"></a>
### Usage Examples	

To list the replica brokers that Zookeeper is aware of:

	zookeeper@dev501:2181:/> kbrokers
    + -------------------------------------------------------------------------- +
    | jmx_port  timestamp          host                          version  port   |
    + -------------------------------------------------------------------------- +
    | 9999      2014-07-31 07:45:23 UTC  dev601.ldaniels528.com  1        9092   |
    | 9999      2014-07-31 07:45:22 UTC  dev602.ldaniels528.com  1        9092   |
    + -------------------------------------------------------------------------- +	

To list all of the Kafka topics that Zookeeper is aware of:

	zookeeper@dev501:2181:/> kls
    + ------------------------------------------------------------------- +
    | name              partition  leader                       version   |
    + ------------------------------------------------------------------- +
    | test.app1.alerts  0          dev601.ldaniels528.com:9092  1         |
    | test.app1.alerts  1          dev602.ldaniels528.com:9092  1         |
    | test.app1.alerts  2          dev601.ldaniels528.com:9092  1         |
    | app1.messages     0          dev602.ldaniels528.com:9092  1         |
    | app1.messages     1          dev601.ldaniels528.com:9092  1         |
    | app1.messages     2          dev602.ldaniels528.com:9092  1         |
    | app1.messages     3          dev601.ldaniels528.com:9092  1         |
    | app1.messages     4          dev602.ldaniels528.com:9092  1         |
    | app1.messages     5          dev601.ldaniels528.com:9092  1         |
    | app1.messages     6          dev602.ldaniels528.com:9092  1         |
    + ------------------------------------------------------------------- +

To see a subset of the topics (matches any topic that starts with the given search term):

	zookeeper@dev501:2181:/> kls test.app1.alerts
    + ------------------------------------------------------------------- +
    | name              partition  leader                       version   |
    + ------------------------------------------------------------------- +
    | test.app1.alerts  0          dev601.ldaniels528.com:9092  1         |
    | test.app1.alerts  1          dev602.ldaniels528.com:9092  1         |
    | test.app1.alerts  2          dev601.ldaniels528.com:9092  1         |
    + ------------------------------------------------------------------- +

To retrieve the start and end offsets and number of messages available for a topic across any number of partitions:

	zookeeper@dev501:2181:/> kstats test.app1.alerts 0 2
    + ------------------------------------------------------------------------ +
    | name              partition  startOffset  endOffset  messagesAvailable   |
    + ------------------------------------------------------------------------ +
    | test.app1.alerts  0          4009955      4009955    0                   |
    | test.app1.alerts  1          3845895      3845895    0                   |
    | test.app1.alerts  2          5322551      5322551    0                   |
    + ------------------------------------------------------------------------ +

To retrieve the list of topics with new messages (since your last query):

    zookeeper@dev501:2181/> kinbound
    + ---------------------------------------------------------------------------------------------------------------- +
    | topic                          partition  startOffset  endOffset    change  msgsPerSec  lastCheckTime            |
    + ---------------------------------------------------------------------------------------------------------------- +
    | test.app1.alerts               8          14172422286  14181448062  22722   1195.9      08/25/2014 08:00PM UTC   |
    | test.app1.alerts               2          14152959137  14161960794  19158   1008.4      08/25/2014 08:00PM UTC   |
    | test.app1.alerts               6          14149557536  14158589929  19079   1004.2      08/25/2014 08:00PM UTC   |
    | test.app1.alerts               0          14149783268  14158884735  19013   1000.7      08/25/2014 08:00PM UTC   |
    | test.app1.alerts               3          14149465416  14158650047  18710   984.8       08/25/2014 08:00PM UTC   |
    | test.app1.alerts               9          14149489857  14158842964  18699   984.2       08/25/2014 08:00PM UTC   |
    | test.app1.alerts               1          14149397624  14158931725  18343   965.5       08/25/2014 08:00PM UTC   |
    | test.app1.alerts               7          14149902016  14159677946  18325   964.5       08/25/2014 08:00PM UTC   |
    | test.app1.alerts               4          14150527623  14159760560  18207   958.3       08/25/2014 08:00PM UTC   |
    | test.app1.alerts               5          14150738074  14160430877  18132   954.4       08/25/2014 08:00PM UTC   |
    | com.shocktrade.quotes.csv      7          58618400     59195800     7400    389.5       08/25/2014 08:00PM UTC   |
    | com.shocktrade.quotes.csv      6          11504800     13458400     2000    105.3       08/25/2014 08:00PM UTC   |
    | com.shocktrade.quotes.csv      6          9856091      10307108     1200    63.2        08/25/2014 08:00PM UTC   |
    + ---------------------------------------------------------------------------------------------------------------- +

To view the Zookeeper keys at the current hierarchy level:

	zookeeper@dev501:2181:/> zls
		consumers
		storm
		controller_epoch
		admin
		controller
		brokers	
			
To change the current Zookeeper hierarchy level:			
			
	zookeeper@dev501:2181:/> zcd brokers
        /brokers
        
Now view the keys at this level:        
    
    zookeeper@dev501:2181:/brokers> zls
        topics
        ids	
        
To list of commands that start with "k":
			
	zookeeper@dev501:2181:/> ?k
    + ------------------------------------------------------------------------------------------------------------------- +
    | command     module  description                                                                                     |
    + ------------------------------------------------------------------------------------------------------------------- +
    | kbrokers    kafka   Returns a list of the brokers from ZooKeeper                                                    |
    | kcommit     kafka   Commits the offset for a given topic and group                                                  |
    | kconsumers  kafka   Returns a list of the consumers from ZooKeeper                                                  |
    | kcursor     kafka   Displays the current message cursor                                                             |
    | kdelta      kafka   Returns a list of deltas between the consumers and topics                                       |
    | kexport     kafka   Writes the contents of a specific topic to a file                                               |
    | kfetch      kafka   Retrieves the offset for a given topic and group                                                |
    | kfetchsize  kafka   Retrieves or sets the default fetch size for all Kafka queries                                  |
    | kfirst      kafka   Returns the first message for a given topic                                                     |
    | kget        kafka   Retrieves the message at the specified offset for a given topic partition                       |
    | kgeta       kafka   Returns the key-value pairs of an Avro message from a topic partition                           |
    | kgetminmax  kafka   Retrieves the smallest and largest message sizes for a range of offsets for a given partition   |
    | kgetsize    kafka   Retrieves the size of the message at the specified offset for a given topic partition           |
    | kimport     kafka   Imports messages into a new/existing topic                                                      |
    | kinbound    kafka   Retrieves a list of topics with new messages (since last query)                                 |
    | klast       kafka   Returns the last message for a given topic                                                      |
    | kls         kafka   Lists all existing topics                                                                       |
    | kmk         kafka   Creates a new topic                                                                             |
    | knext       kafka   Attempts to retrieve the next message                                                           |
    | koffset     kafka   Returns the offset at a specific instant-in-time for a given topic                              |
    | kprev       kafka   Attempts to retrieve the message at the previous offset                                         |
    | kpush       kafka   Publishes a message to a topic                                                                  |
    | krm         kafka   Deletes a topic (DESTRUCTIVE)                                                                   |
    | kscana      kafka   Scans a range of messages verifying conformance to an Avro schema                               |
    | kstats      kafka   Returns the partition details for a given topic                                                 |
    + ------------------------------------------------------------------------------------------------------------------- +

To see the current offsets for all consumer IDs:

    zookeeper@vsccrtc201-brn1:2181/> kconsumers
    + -------------------------------------------------------------------------------------------------------------------------- +
    | consumerId                                               topic                                      partition  offset      |
    + -------------------------------------------------------------------------------------------------------------------------- +
    | kafka-to-file-1407952872635-1811322114                   Verisign.test.iota.rtc.listener.all.hydra  0          26006370    |
    | kafka-to-file-1407952872635-1811322114                   Verisign.test.iota.rtc.listener.all.hydra  1          23751039    |
    | kafka-to-file-1407952872635-1811322114                   Verisign.test.iota.rtc.listener.all.hydra  2          22611530    |
    | kafka-to-file-1407952872635-1811322114                   Verisign.test.iota.rtc.listener.all.hydra  3          24462662    |
    | kafka-to-file-1407952872635-1811322114                   Verisign.test.iota.rtc.listener.all.hydra  4          25603795    |
    | kafka-to-file-1407952872635-1811322114                   Verisign.test.iota.rtc.listener.all.hydra  5          23941687    |
    | kafka-to-file-1407952872635-1811322114                   Verisign.test.iota.rtc.listener.all.hydra  6          25435327    |
    | kafka-to-file-1407952872635-1811322114                   Verisign.test.iota.rtc.listener.all.hydra  7          23741720    |
    | kafka-to-file-1407952872635-1811322114                   Verisign.test.iota.rtc.listener.all.hydra  8          21999410    |
    | kafka-to-file-1407952872635-1811322114                   Verisign.test.iota.rtc.listener.all.hydra  9          24762782    |
    | kafka-to-file-1407953236905-602150863                    Verisign.test.iota.rtc.listener.all.hydra  0          26006370    |
    | kafka-to-file-1407953236905-602150863                    Verisign.test.iota.rtc.listener.all.hydra  1          23751039    |
    | kafka-to-file-1407953236905-602150863                    Verisign.test.iota.rtc.listener.all.hydra  2          22611530    |
    | kafka-to-file-1407953236905-602150863                    Verisign.test.iota.rtc.listener.all.hydra  3          24462662    |
    | kafka-to-file-1407953236905-602150863                    Verisign.test.iota.rtc.listener.all.hydra  4          25603795    |
    | kafka-to-file-1407953236905-602150863                    Verisign.test.iota.rtc.listener.all.hydra  5          23941687    |
    | kafka-to-file-1407953236905-602150863                    Verisign.test.iota.rtc.listener.all.hydra  6          25435327    |
    | kafka-to-file-1407953236905-602150863                    Verisign.test.iota.rtc.listener.all.hydra  7          23766520    |
    | kafka-to-file-1407953236905-602150863                    Verisign.test.iota.rtc.listener.all.hydra  8          22205610    |
    | kafka-to-file-1407953236905-602150863                    Verisign.test.iota.rtc.listener.all.hydra  9          24762782    |
    + -------------------------------------------------------------------------------------------------------------------------- +

