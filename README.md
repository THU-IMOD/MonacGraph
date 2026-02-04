# MonacGraph
A graph database system based on the TinkerPop framework, supporting second-order logic.
The storage backend is our community structure based graph storage system, i.e., LSM-Community.

## Demo Video

Watch the full demonstration of MonacGraph:  
https://www.youtube.com/watch?v=Eezdq9tzbJE

## MonacGraph Build & Run Guide

This guide explains how to compile the native LSM storage engine, build the Java application, and run the server/clients.

### 1. Build Native Storage Engine (Rust)

First, compile the Rust project and copy the native library to the Java resource directory.

```bash
# 1. Enter the Rust sub-project directory
cd lsm-community

# 2. Compile the optimized release version
cargo build -p lsm-community-java --release

# 3. Copy the native library to Java resources (run ONLY the command for your OS)
# Windows
mkdir -p ../src/main/resources/storage/windows/
cp ./target/release/lsm_community_java.dll ../src/main/resources/storage/windows/
# Linux
mkdir -p ../src/main/resources/storage/linux/
cp ./target/release/liblsm_community_java.so ../src/main/resources/storage/linux/
# macOS
mkdir -p ../src/main/resources/storage/macos/
cp ./target/release/liblsm_community_java.dylib ../src/main/resources/storage/macos/

# 4. Return to the project root directory
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
java -cp target/Gremmunity-1.0-SNAPSHOT.jar com.graph.rocks.example.MonacGraphServer
```

### 4. Running Web Client
Open a new terminal window to run the web-based visualization client.

**Prerequisites**: Ensure Node.js (v16+) and npm are installed on your system.

Run the Web Client:

```bash
# 1. Navigate to the demo (web client) subdirectory
cd web-client

# 2. Install all required Node.js dependencies for the web client
npm install

# 3. Start the development server (runs on http://localhost:5173 by default)
npm run dev
```

**Access the Web Interface**:
After the server starts successfully, you will see the local access URL in the terminal: `http://localhost:5173/`.

**Method 1 (Click the Link)**:
Hold `Ctrl` (Windows/Linux) or `Cmd` (macOS) and click the URL directly in the terminal to open it in your default browser.

**Method 2 (Command Line Instruction)**:
Run the following command in a new terminal to open the URL automatically:

```bash
# Open the web interface automatically (run ONLY the command for your OS)
# Windows (PowerShell)
start http://localhost:5173/
# Linux
xdg-open http://localhost:5173/
# macOS
open http://localhost:5173/
```


### 5. Custom Usage
You can also build your own client applications. Since this project is compatible with Apache TinkerPop, you can use the Gremlin query language to perform graph traversals and queries against the server.