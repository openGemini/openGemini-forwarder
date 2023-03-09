# openGemini-forwarder
High-performance data access middleware. For example, Kafka data can be consumed, converted and written into openGemini.
#### Currently, it is only a framework, and the specific functions will take some time to complete.


## Quick Start

This section mainly contains the following:

- How to compile forwarder source code
- How to run forwarder 



### Compiling environment information

[GO](https://golang.org/dl/) version v1.18+

[Python](https://www.python.org/downloads/) version v3.7+

**How to set GO environment variables**

Open ~/.profile configuration file and add the following configurations to the end of the file:

```
export GOPATH=/path/to/dir
export GOBIN=$GOPATH/bin
export GO111MODULE=on
export GONOSUMDB=*
export GOSUMDB=off
```



### Compiling

1. Clone source codes from Github

```
> cd $GOPATH
> mkdir -p {pkg,bin,src}
> cd src
> git clone https://github.com/openGemini/openGemini-forwarder.git
```

1. Enter the home directory

```
> cd openGemini-forwarder
```

1. Compiling

```
> python build.py --clean
```

The compiled binary file is in the build directory

```
> ls build
ts-forwarder
```



### Configuration

The configuration file is in the config directory. 

### Run forwarder

Standalone operation

```
> mkdir -p /tmp/openGemini/logs/
```

Refer to cluster deployments in [User Guide](http://opengemini.org/docs)

#### Deploy openGemini

Refer to openGemini [User Guide](http://opengemini.org/docs)



#### Deploy kafka

Refer to kafka doc



#### Create kafka topic

Refer to kafka doc

```
bin/kafka-topics.sh --create --bootstrap-server 127.0.0.1:9093 --replication-factor 1 --partitions 1 --topic test-xx
```



#### Start forwarder

```
build/ts-forwarder --config config/forwarder.conf
```

