# pg-broker

Simple event broker on top of PostgreSQL using LISTEN/NOTIFY mechanism.

## Prerequisite :

1. Java 19
2. Start PostgreSQL docker container :

    ```console
   docker run --name pg-broker -p 5432:5432 -e POSTGRES_DB=pg-broker -e POSTGRES_USER=pg-broker -e POSTGRES_PASSWORD=kewk -it postgres
    ```
   
## Server example

Build an executable fat-jar
 ```console
mvn package 
```

Start a broker instance :
```console
java.exe --enable-preview -jar pg-broker-server/target/pg-broker-server-1.0-SNAPSHOT.jar 1337
``` 

## Client examples

Create a new consumer and subscribe to a topic :
```java
 ConsumerClient.builder()
        .hostname("127.0.0.1")
        .port(1337)
        .build()
        .connect()
        .subscribe("topic", event -> {
            logger.info("Received new event for topic '{}' : {}", topic, event);
        });
```

Create a new producer and send an event :
```java
ProducerClient producer = ProducerClient.builder()
                .hostname("127.0.0.1")
                .port(1337)
                .build()
                .connect();

producer.sendEvent("topic", "Hello there");
```