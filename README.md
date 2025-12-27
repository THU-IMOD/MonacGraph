# Gremmunity
A graph database system based on the TinkerPop framework, supporting second-order logic.
The storage backend is our community structure based graph storage system, i.e., LSM-Community.


## Gremmunity Build & Run Guide

This guide explains how to compile the native LSM storage engine, build the Java application, and run the server/clients.

### 1. Build Native Storage Engine (Rust)

First, compile the Rust project and copy the native library to the Java resource directory.

*Note: This example assumes a macOS environment (`.dylib`). For Windows/Linux, adjust the file extension and destination folder accordingly.*

```bash
# 1. Enter the rust sub-project
cd lsm-community

# 2. Compile the release version
cargo build -p lsm-community-java --release

# 3. Copy the native library to the Java resources folder
cp ./target/release/liblsm_community_java.dylib ../src/main/resources/storage/macos

# 4. Return to the project root
cd ..
```

### 2. Build Java Application
Compile the Java project and package it into a JAR file.

```bash
mvn clean package
```

### 3. Running the Server
Start the Gremlin Server. Ensure the server is fully started (wait for the port binding log) before running any clients.

```bash
java -cp target/Gremmunity-1.0-SNAPSHOT.jar com.graph.rocks.example.GremmunityServer
```

### 4. Running Clients
Open a new terminal window to run the example clients.

Run the Standard Client:

```bash
java -cp target/Gremmunity-1.0-SNAPSHOT.jar com.graph.rocks.example.GremmunityClient

#or

java -cp target/Gremmunity-1.0-SNAPSHOT.jar com.graph.rocks.example.BatchImportClient
```


### 5. Custom Usage
You can also build your own client applications. Since this project is compatible with Apache TinkerPop, you can use the Gremlin query language to perform graph traversals and queries against the server.