<<<<<<< HEAD
# kinesis-sample
Sample Producer and Consumer for AWS Kinesis Data Streams


### Sample Producer

The sample producer will put data in the the stream "kclnodejssample" in the region us-west-2. It will also crate the stream if it doesn't already exist.
The sample producer can be run using the following maven command using the exec plugin:

`mvn exec:java -Dexec.mainClass="com.aws.kinesis.sample.AmazonKinesisRecordProducerSample" -Djava.util.logging.config.file=src/main/resources/application.properties`

The producer puts the data in the following format in the stream:

"testData-2018-05-30T19:40:48.378" 

The timestamp in the data will be the local timestamp.

### Sample Consumer

The sample consumer gets the data from the stream "kclnodejssample" in the region us-west-2 and outputs it to the standard output.
The sample consumer can be run using the following maven command using the exec plugin:

`mvn exec:java -Dexec.mainClass="com.aws.kinesis.sample.AmazonKinesisRecordConsumerSample" -Djava.util.logging.config.file=src/main/resources/application.properties`


### Logging

There are two branches in this repository. The branch dev is setup to use JDK14 logger and the branch log4j is setup to use log4j logging. 
The '-Djava.util.logging.config.file=src/main/resources/application.properties' argument in the main command should only be used when runnig the applications from the dev branch.

The src/main/resources/application.properties file contains the configuration for for the JDK14 logger. Currently it is configured to emit logs both to the console and the file "log-test.log" in the root directory.
The log level is set to 'ALL' for both the handlers but can be changed to the desired level.

The file 'src/main/resources/commons-logging.properties' defines which logging implementation the commons logging library should use

The src/main/resources/log4j.properties file contains the configuration for for the log4j logger. Currently it is configured to emit logs both to the console and the file "application.log" in the root directory.The log level is set to 'Info' for both the appenders but can be changed to the desired level.
=======
# kinesis-sample
>>>>>>> refs/remotes/origin/master
