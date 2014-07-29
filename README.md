# Verify

Swiss-Army-Knife Utility for managing Kafka topics and ZooKeeper properties

Table of Contents

* <a href="#Motivations">Motivations</a>
* <a href="#Building-the-code">Building the code</a>
* <a href="#Usage">Run the application</a>

<a name="Motivations"></a>

### Motivations

The motivations behind creating `Verify` are simple; testing, verifying and managing Kafka topics can be an
arduous task. The goal of this project is to ease the pain of developing applications that make use of 
Kafka/Storm/ZooKeeper-based.

<a name="Building-the-code"></a>

### Building the code

    $ ./sbt clean package
    
<a name="Usage"></a>

### Run the application

java -jar verify.jar <zookeperHost>



    
    
