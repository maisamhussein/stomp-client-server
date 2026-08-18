# STOMP Client-Server System

A concurrent client-server application developed as part of a Systems Programming course.

The project consists of a **Java server** and a **C++ client** that communicate over **TCP** using the **STOMP messaging protocol**. The system supports multiple clients, topic-based communication, subscriptions, and message delivery.

## Technologies

* Java
* C++
* TCP/IP
* STOMP Protocol
* Multithreading
* Maven
* Boost.Asio
* Linux

## Project Structure

```text
stomp-client-server/
├── client/     # C++ client implementation
├── server/     # Java server implementation
├── data/       # Supporting data files
└── .gitignore
```

## Key Features

* Client-server communication over TCP
* STOMP-based messaging protocol
* Support for multiple concurrent clients
* User login and connection management
* Topic subscription and unsubscription
* Message publishing and delivery
* Concurrent server-side connection handling
* C++ client implemented using Boost.Asio
* Java server supporting different connection-handling models

## Server

The server is implemented in **Java** and handles multiple client connections concurrently.

The implementation includes components for:

* Connection management
* STOMP message encoding and decoding
* Processing STOMP commands
* Managing subscriptions and message delivery
* Blocking and non-blocking connection handling
* Reactor-based server architecture

## Client

The client is implemented in **C++** and communicates with the server using TCP.

It handles:

* Connecting to the server
* Sending STOMP commands
* Receiving and processing server messages
* Managing subscriptions
* User commands and session state

## Build Tools

The project uses:

* **Maven** for the Java server
* **Make** for the C++ client

## Academic Project

Developed as part of the **Systems Programming** course at Ben-Gurion University.

## How to Run

### Server

Compile the Java server:

```bash
cd server
mvn compile
```

Run using Thread-Per-Client:

```bash
java -cp target/classes bgu.spl.net.impl.stomp.StompServer 7777 tpc
```

Or run using the Reactor model:

```bash
java -cp target/classes bgu.spl.net.impl.stomp.StompServer 7777 reactor 4
```

### Client

Build the C++ client:

```bash
cd client
mkdir -p bin
make
```

Run the client:

```bash
./bin/StompWCIClient
```
