## Introduction
This module was created as part of a University project
at [University of Applied Sciences and Arts Northwestern Switzerland FHNW](http://www.fhnw.ch).

The Python3 module is for use with Internet of Things (IoT) ad-hoc networks.
The Internet of Things (IoT) comprises of networks of communicating sensors which provide data for analysis
and can be used to automate processes. Ad-hoc networks are self-organizing networks of nodes which communicate with
each other directly across a dynamically formed mesh.

A task often completed is the acquisition of data from multiple sensors on a wired or wireless Local Area Network (LAN).

The software supports a generalised data pipeline. This pipeline is a simple data flow from a data source, to a data sink.
This data flow is considered as a series of data units, or jobs, which may be processed at any point from source to sink.

## Module
The library uses the [Pyre](https://github.com/zeromq/pyre) and the [ZeroMQ](http://zeromq.org/) messaging library to provides the user with classes which do the following:

* create a dynamic ad-hoc network automatically, without pre-configuration
* elect a coordinator to manage creation and processing of data in a distributed fashion
* executes user-defined source, processing and sink functions

## Requirements
The module requires the following non-core modules:
* zeromq
* pyre

Some plugins require the following non-core modules:
* psycopg2
* paho.mqtt.client


## Basic use

### Creation of Named Groups
The software should be running on each LAN node. A node object should be created first;

~~~~
# Create Node for this device
node = ad_hoc_network.Node()
~~~~

A named group can be created which carries out discovery of other nodes, leader election and carries out the
data creation and processing:

~~~~
# Create new group called "Thermometers" for this device which pretends to monitor room temperature
# and prints average to sink node terminal
node.new_group("Thermometers", FakeTemperature, AverageTemperature, plugins.PrintAsText, None, 3, is_sink)
~~~~
The `new_group()` function takes the following parameters:

* `grp_name` - *text* - The name of the group
* `source_function` - The user defined function (see below) used to create data at each group node
* `process_function` - The user defined function (see below) used by the processor on all received source data
* `sink_function` - The user defined function (see below) used by the sink node on the processed data
* `sink_extra_params` - *keyword argument dictionary* - Additional parameters which will be passed to the sink function
* `interval` - *Integer/Float, default=1* - The interval between creation of jobs
* `is_sink` - *boolean, default=False* - Whether the node is to provide sink capability to the group
(perhaps the sink has a connection to the internet)

### User-defined Functions

Each node has been provided with a Python function explaining how to create source data, how to process data and what
to do with the data which has been processed, called the Sink function.

As an example, the Source function could be a temperature measurement, the Processing function could calculate an average
temperature from all the Source temperatures, and the Sink function could save the average temperature to a database.

The module is provided with a suite of pre-programmed source, processing and sink functions for typical scenarios.
These 'Plugin functions' may be extended by the user, depending on the desired application.

#### Source:
Should accept the Job ID upon initialisation.

Should have a method `execute()` which returns binary data.

###### Plugin objects
* `ShellSource` - Execution of shell command

#### Processing:
Should accept the received source data (list containing binary data) and Job ID upon initialisation.

Should have a method `execute()` which returns binary data.

###### Plugin objects
* `ShellProcess` - Execution of shell command

#### Sink:
Should accept the processed data and Job ID upon initialisation.

Should have a method `execute()` which does something with the data.

###### Plugin objects
* `ShellSink` - Execution of shell command
* `PrintAsText` - Print data as text to console/output
* `SaveLocal` - Save data locally
* `SendMQTT` - Publish MQTT message
* `PushZMQ` / `PubZMQ` - Send as ZMQ message (Push/Publish)
* `SendPostgres` - Update Postgres database
* `SendSQLite` - Update SQLite database
* `SendSMTP` - Send SMTP E-Mail

The provision of a suite of extendable plugins provides both convenience and flexibility for the user. The user can realise a wide range of network applications which conforms to the Source-Process-Sink model.

Multiple groups can be created, each with their own functions.

## Example
The library includes an example application *example.py* which generates a random temperature which is sent to a processing node. The processing node calculates an average temperature which is sent to the sink node where the average value is displayed.

## Performance
The module has been tested withe the following variable combinations:
* Interval : 0.01 - 20s
* Payload : 100 Bytes – 5’000kB
* Number of Nodes
 * Single host : 1-20
 * WLAN : 1 – 7

The performance on a single machine was excellent. Minimum recommended intervals for different data sizes are shown
on the [image](images/LocalhostMinInterval.png).
On a wireless network test, data above 1kB failed to be completed. This is likely due to bottlenecking across the
wireless network and the user should test the module for his/her own application.

### Improvements
It is intended to introduce a maximum waiting time at the processor, instead of waiting for all node data to arrive.
This should help to improve the stability of the library where bandwidth is slow or nodes are lost.