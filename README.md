# Verify

Verify is a figurative Swiss-Army-Knife for inspecting/managing Kafka topics and ZooKeeper properties.

Table of Contents

* <a href="#Motivations">Motivations</a>
* <a href="#Development">Development</a>
	* <a href="#build-requirements">Build Requirements</a>
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

* SBT 0.13.5+

<a name="building-the-code"></a>
### Building the code

    $ sbt clean package
    
<a name="testing-the-code"></a>    
### Running the tests

    $ sbt clean test    

<a name="Running-the-app"></a> 
### Run the application

	$ java -jar verify.jar <zookeperHost>

<a name="usage"></a>
### Usage Examples	

	$ kbrokers
		+ -------------------------------------------------------------------------- +
		| jmx_port  timestamp          host                          version  port   |
		+ -------------------------------------------------------------------------- +
		| 9999      2014-07-31 07:45:23 UTC  dev601.ldaniels528.com  1        9092   |
		| 9999      2014-07-31 07:45:22 UTC  dev602.ldaniels528.com  1        9092   |
		+ -------------------------------------------------------------------------- +	

	$ kls
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

	$ kls test.app1.alerts
		+ ------------------------------------------------------------------------- +
		| name              partition  leader                             version   |
		+ ------------------------------------------------------------------------- +
		| test.app1.alerts  0          vsccrtc204-brn1.rtc.vrsn.com:9092  1         |
		| test.app1.alerts  1          vsccrtc205-brn1.rtc.vrsn.com:9092  1         |
		| test.app1.alerts  2          vsccrtc204-brn1.rtc.vrsn.com:9092  1         |
		+ ------------------------------------------------------------------------- +

	$ kstats test.app1.alerts 0 2
		+ ------------------------------------------------------------------------ +
		| name              partition  startOffset  endOffset  messagesAvailable   |
		+ ------------------------------------------------------------------------ +
		| test.app1.alerts  0          4009955      4009955    0                   |
		| test.app1.alerts  1          3845895      3845895    0                   |
		| test.app1.alerts  2          5322551      5322551    0                   |
		+ ------------------------------------------------------------------------ +

	$ zls
		consumers
		storm
		controller_epoch
		admin
		controller
		brokers	
		