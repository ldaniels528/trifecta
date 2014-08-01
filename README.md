# Verify

Swiss-Army-Knife Utility for managing Kafka topics and ZooKeeper properties

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

The motivations behind creating `Verify` are simple; testing, verifying and managing Kafka topics can be an
arduous task. The goal of this project is to ease the pain of developing applications that make use of 
Kafka/Storm/ZooKeeper-based.

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
		+ -------------------------------------------------------------------------------- +
		| jmx_port  timestamp                host                          version  port   |
		+ -------------------------------------------------------------------------------- +
		| 9999      2014-07-31 07:45:23 UTC  dev601.vmx00.ldaniels528.com  1        9092   |
		| 9999      2014-07-31 07:45:22 UTC  dev602.vmx00.ldaniels528.com  1        9092   |
		+ -------------------------------------------------------------------------------- +	

	$ kls
		+ ----------------------------------------------------------------------------- +
		| name                  partition  leader                             version   |
		+ ----------------------------------------------------------------------------- +
		| test.ldaniels.alerts  0          dev601.vmx00.ldaniels528.com:9092  1         |
		| test.ldaniels.alerts  1          dev602.vmx00.ldaniels528.com:9092  1         |
		| test.ldaniels.alerts  2          dev601.vmx00.ldaniels528.com:9092  1         |
		| test.ldaniels.alerts  3          dev602.vmx00.ldaniels528.com:9092  1         |
		| test.ldaniels.alerts  4          dev601.vmx00.ldaniels528.com:9092  1         |
		| test.ldaniels.alerts  5          dev602.vmx00.ldaniels528.com:9092  1         |
		| test.ldaniels.alerts  6          dev601.vmx00.ldaniels528.com:9092  1         |
		| test.ldaniels.alerts  7          dev602.vmx00.ldaniels528.com:9092  1         |
		| test.ldaniels.alerts  8          dev601.vmx00.ldaniels528.com:9092  1         |
		| test.ldaniels.alerts  9          dev602.vmx00.ldaniels528.com:9092  1         |
		+ ----------------------------------------------------------------------------- +

	$ zls
		consumers
		storm
		controller_epoch
		admin
		controller
		brokers	
	

	
		