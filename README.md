# Verify

Verify is a figurative Swiss-Army-Knife for inspecting/managing Kafka topics and ZooKeeper properties.

Table of Contents

* <a href="#Motivations">Motivations</a>
* <a href="#Development">Development</a>
	* <a href="#build-requirements">Build Requirements</a>
	* <a href="#getting-the-code">Getting the code</a>
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

* SBT 0.13+

<a name="getting-the-code"></a>
### Getting the code

    $ git clone git@github.com:ldaniels528/verify.git

<a name="configuring-your-ide"></a>
### Configuring the project for your IDE

#### Eclipse project
    $ sbt update eclipse
    
#### Intellij Idea project
    $ sbt update gen-idea

<a name="building-the-code"></a>
### Building the code

    $ sbt clean assembly
    
<a name="testing-the-code"></a>    
### Running the tests

    $ sbt test    

<a name="Running-the-app"></a> 
### Run the application

	$ java -jar verify.jar <zookeperHost>

<a name="usage"></a>
### Usage Examples	

	ldaniels@localhost:2181:/> kbrokers
		+ -------------------------------------------------------------------------- +
		| jmx_port  timestamp          host                          version  port   |
		+ -------------------------------------------------------------------------- +
		| 9999      2014-07-31 07:45:23 UTC  dev601.ldaniels528.com  1        9092   |
		| 9999      2014-07-31 07:45:22 UTC  dev602.ldaniels528.com  1        9092   |
		+ -------------------------------------------------------------------------- +	

	ldaniels@localhost:2181:/> kls
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

	ldaniels@localhost:2181:/> kls test.app1.alerts
		+ ------------------------------------------------------------------- +
		| name              partition  leader                       version   |
		+ ------------------------------------------------------------------- +
		| test.app1.alerts  0          dev601.ldaniels528.com:9092  1         |
		| test.app1.alerts  1          dev602.ldaniels528.com:9092  1         |
		| test.app1.alerts  2          dev601.ldaniels528.com:9092  1         |
		+ ------------------------------------------------------------------- +

	ldaniels@localhost:2181:/> kstats test.app1.alerts 0 2
		+ ------------------------------------------------------------------------ +
		| name              partition  startOffset  endOffset  messagesAvailable   |
		+ ------------------------------------------------------------------------ +
		| test.app1.alerts  0          4009955      4009955    0                   |
		| test.app1.alerts  1          3845895      3845895    0                   |
		| test.app1.alerts  2          5322551      5322551    0                   |
		+ ------------------------------------------------------------------------ +

	ldaniels@localhost:2181:/> zls
		consumers
		storm
		controller_epoch
		admin
		controller
		brokers	
			
	ldaniels@localhost:2181:/> zcd brokers
        /brokers
    
    ldaniels@localhost:2181:/brokers> zls
        topics
        ids	
			
	ldaniels@localhost:2181:/> ?k
        + -------------------------------------------------------------------------------------------------------------------------------- +
        | command      module  description                                                                                                 |
        + -------------------------------------------------------------------------------------------------------------------------------- +
        | kavrochk     kafka   Verifies that a set of messages (specific offset range) can be read by the specified schema                 |
        | kavrofields  kafka   Returns the fields of an Avro message from a Kafka topic                                                    |
        | kbrokers     kafka   Returns a list of the registered brokers from ZooKeeper                                                     |
        | kcommit      kafka   Commits the offset for a given topic and group                                                              |
        | kcount       kafka   Returns the number of messages available for a given topic                                                  |
        | kdump        kafka   Dumps the contents of a specific topic [as binary] to the console                                           |
        | kdumpa       kafka   Dumps the contents of a specific topic [as Avro] to the console                                             |
        | kdumpf       kafka   Dumps the contents of a specific topic to a file                                                            |
        | kdumpr       kafka   Dumps the contents of a specific topic [as raw ASCII] to the console                                        |
        | kfetch       kafka   Retrieves the offset for a given topic and group                                                            |
        | kfetchsize   kafka   Retrieves or sets the default fetch size for all Kafka queries                                              |
        | kfirst       kafka   Returns the first offset for a given topic                                                                  |
        | kget         kafka   Retrieves the message at the specified offset for a given topic partition                                   |
        | kgetmaxsize  kafka   Retrieves the size of the largest message for the specified range of offsets for a given topic partition    |
        | kgetminsize  kafka   Retrieves the size of the smallest message for the specified range of offsets for a given topic partition   |
        | kgetsize     kafka   Retrieves the size of the message at the specified offset for a given topic partition                       |
        | kimport      kafka   Imports data into a new/existing topic                                                                      |
        | klast        kafka   Returns the last offset for a given topic                                                                   |
        | kls          kafka   Lists all existing topics                                                                                   |
        | kmk          kafka   Returns the system time as an EPOC in milliseconds                                                          |
        | koffset      kafka   Returns the offset at a specific instant-in-time for a given topic                                          |
        | kpush        kafka   Publishes a message to a topic                                                                              |
        | krm          kafka   Deletes a topic                                                                                             |
        | kstats       kafka   Returns the parition details for a given topic                                                              |
        + -------------------------------------------------------------------------------------------------------------------------------- +