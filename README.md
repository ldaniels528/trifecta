Verify
=======

Verify is a figurative Swiss-Army-Knife for inspecting/managing Kafka topics and ZooKeeper properties.

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

* Java SDK 1.7
* [Tabular] (https://github.com/ldaniels528/tabular)
* [SBT 0.13+] (http://www.scala-sbt.org/download.html)

<a name="configuring-your-ide"></a>
### Configuring the project for your IDE

#### Eclipse project
    $ sbt eclipse
    
#### Intellij Idea project
    $ sbt gen-idea

<a name="building-the-code"></a>
### Building the code

    $ sbt assembly
    
<a name="testing-the-code"></a>    
### Running the tests

    $ sbt test    

<a name="Running-the-app"></a> 
### Run the application

	$ java -jar verify.jar <zookeperHost>

<a name="usage"></a>
### Usage Examples	

To list the replica brokers that Zookeeper is aware of:

	zookeeper@localhost:2181:/> kbrokers
    + -------------------------------------------------------------------------- +
    | jmx_port  timestamp          host                          version  port   |
    + -------------------------------------------------------------------------- +
    | 9999      2014-07-31 07:45:23 UTC  dev601.ldaniels528.com  1        9092   |
    | 9999      2014-07-31 07:45:22 UTC  dev602.ldaniels528.com  1        9092   |
    + -------------------------------------------------------------------------- +	

To list all of the Kafka topics that Zookeeper is aware of:

	zookeeper@localhost:2181:/> kls
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

	zookeeper@localhost:2181:/> kls test.app1.alerts
    + ------------------------------------------------------------------- +
    | name              partition  leader                       version   |
    + ------------------------------------------------------------------- +
    | test.app1.alerts  0          dev601.ldaniels528.com:9092  1         |
    | test.app1.alerts  1          dev602.ldaniels528.com:9092  1         |
    | test.app1.alerts  2          dev601.ldaniels528.com:9092  1         |
    + ------------------------------------------------------------------- +

To retrieve the start and end offsets and number of messages available for a topic across any number of partitions:

	zookeeper@localhost:2181:/> kstats test.app1.alerts 0 2
    + ------------------------------------------------------------------------ +
    | name              partition  startOffset  endOffset  messagesAvailable   |
    + ------------------------------------------------------------------------ +
    | test.app1.alerts  0          4009955      4009955    0                   |
    | test.app1.alerts  1          3845895      3845895    0                   |
    | test.app1.alerts  2          5322551      5322551    0                   |
    + ------------------------------------------------------------------------ +

To view the Zookeeper keys at the current hierarchy level:

	zookeeper@localhost:2181:/> zls
		consumers
		storm
		controller_epoch
		admin
		controller
		brokers	
			
To change the current Zookeeper hierarchy level:			
			
	zookeeper@localhost:2181:/> zcd brokers
        /brokers
        
Now view the keys at this level:        
    
    zookeeper@localhost:2181:/brokers> zls
        topics
        ids	
        
To list of commands that start with "k":
			
	zookeeper@localhost:2181:/> ?k
    + -------------------------------------------------------------------------------------------------------------- +
    | command      module  description                                                                               |
    + -------------------------------------------------------------------------------------------------------------- +
    | kbrokers     kafka   Returns a list of the registered brokers from ZooKeeper                                   |
    | kchka        kafka   Verifies that a range of messages can be read by a given Avro schema                      |
    | kcommit      kafka   Commits the offset for a given topic and group                                            |
    | kcount       kafka   Returns the number of messages available for a given topic                                |
    | kdump        kafka   Dumps the contents of a specific topic [as binary] to the console                         |
    | kdumpa       kafka   Dumps the contents of a specific topic [as Avro] to the console                           |
    | kdumpf       kafka   Dumps the contents of a specific topic to a file                                          |
    | kdumpr       kafka   Dumps the contents of a specific topic [as raw ASCII] to the console                      |
    | kfetch       kafka   Retrieves the offset for a given topic and group                                          |
    | kfetchsize   kafka   Retrieves or sets the default fetch size for all Kafka queries                            |
    | kfirst       kafka   Returns the first offset for a given topic                                                |
    | kget         kafka   Retrieves the message at the specified offset for a given topic partition                 |
    | kgeta        kafka   Returns the key-value pairs of an Avro message from a topic partition                     |
    | kgetmaxsize  kafka   Retrieves the size of the largest message for a range of offsets for a given partition    |
    | kgetminsize  kafka   Retrieves the size of the smallest message for a range of offsets for a given partition   |
    | kgetsize     kafka   Retrieves the size of the message at the specified offset for a given topic partition     |
    | kimport      kafka   Imports messages into a new/existing topic                                                |
    | kinbound     kafka   Retrieves a list of topics with new messages (since last query)                           |
    | klast        kafka   Returns the last offset for a given topic                                                 |
    | kls          kafka   Lists all existing topics                                                                 |
    | kmk          kafka   Creates a new topic                                                                       |
    | koffset      kafka   Returns the offset at a specific instant-in-time for a given topic                        |
    | kpush        kafka   Publishes a message to a topic                                                            |
    | krm          kafka   Deletes a topic (DESTRUCTIVE)                                                             |
    | kstats       kafka   Returns the parition details for a given topic                                            |
    + -------------------------------------------------------------------------------------------------------------- +
