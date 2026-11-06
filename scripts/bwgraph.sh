#!/usr/bin/env bash
# Build/test/start MonacGraph with the local Bw-Graph JNI backend.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${JAVA_HOME:?Set JAVA_HOME to a Java 17+ JDK}"
export PATH="$JAVA_HOME/bin:$PATH"
action="${1:-test}"
case "$(uname -s)" in
  Darwin) suffix=dylib; platform=macos ;;
  Linux) suffix=so; platform=linux ;;
  *) echo 'Bw-Graph native backend requires macOS or Linux.' >&2; exit 1 ;;
esac
case "$action" in
  build)
    : "${BWGRAPH_SOURCE:=$PWD/bw-cloud-graph}"
    [[ -f "$BWGRAPH_SOURCE/tools/build.sh" ]] || {
      echo "Initialize the storage submodule: git submodule update --init --recursive" >&2; exit 1;
    }
    BWGRAPH_SOURCE="$(cd "$BWGRAPH_SOURCE" && pwd)"
    # Refuse an ABI declaration mismatch rather than overwriting Java source silently.
    cmp "$BWGRAPH_SOURCE/bindings/java/db/monacgraph/jni/BwGraphJNI.java" src/main/java/db/monacgraph/jni/BwGraphJNI.java
    build="${BWGRAPH_BUILD_DIR:-$BWGRAPH_SOURCE/build-monac-jni}"
    (cd "$BWGRAPH_SOURCE" && bash tools/build.sh --build-dir "$build" -t release --jni -j "${JOBS:-8}")
    mkdir -p "src/main/resources/storage/$platform"
    cp "$build/lib/libbwgraph_jni.$suffix" "src/main/resources/storage/$platform/"
    cargo build --manifest-path lsm-community/Cargo.toml --locked --release -p lsm-community-java
    cp "lsm-community/target/release/liblsm_community_java.$suffix" "src/main/resources/storage/$platform/"
    cmake -S subgraph -B subgraph/build-monac-integration -DCMAKE_BUILD_TYPE=Release
    cmake --build subgraph/build-monac-integration --target subgraph_matching -j "${JOBS:-8}"
    cp "subgraph/build-monac-integration/libsubgraph_matching.$suffix" "src/main/resources/storage/$platform/"
    mvn package -Dbwgraph.integration=true
    ;;
  test)
    args=(-Dbwgraph.integration=true)
    if [[ -n "${BWGRAPH_JNI_LIBRARY:-}" ]]; then args+=("-Dbwgraph.jni.path=$BWGRAPH_JNI_LIBRARY"); fi
    mvn test "${args[@]}"
    ;;
  start)
    # Never run from Maven's mutable output: rebuilding it breaks lazy class
    # loading in an already running JVM, including its graceful shutdown hooks.
    jar=target/Gremmunity-1.0-SNAPSHOT.jar
    [[ -f "$jar" ]] || { echo "Build $jar first." >&2; exit 1; }
    digest=$(shasum -a 256 "$jar" | awk '{print $1}')
    mkdir -p target/runtime
    snapshot="target/runtime/$digest.jar"
    if [[ ! -f "$snapshot" ]]; then
      temporary=$(mktemp target/runtime/.launch-XXXXXX)
      cp "$jar" "$temporary"
      copied=$(shasum -a 256 "$temporary" | awk '{print $1}')
      [[ "$copied" = "$digest" ]] || {
        echo 'JAR changed during launch; finish the build and retry.' >&2; exit 1;
      }
      chmod 444 "$temporary"
      mv "$temporary" "$snapshot"
    fi
    args=(-cp "$snapshot")
    if [[ -n "${BWGRAPH_JNI_LIBRARY:-}" ]]; then args+=("-Dbwgraph.jni.path=$BWGRAPH_JNI_LIBRARY"); fi
    jsig="$JAVA_HOME/lib/libjsig.$suffix"
    [[ -f "$jsig" ]] || { echo "Missing $jsig" >&2; exit 1; }
    if [[ "$platform" = macos ]]; then export DYLD_INSERT_LIBRARIES="$jsig${DYLD_INSERT_LIBRARIES:+:$DYLD_INSERT_LIBRARIES}";
    else export LD_PRELOAD="$jsig${LD_PRELOAD:+:$LD_PRELOAD}"; fi
    exec "$JAVA_HOME/bin/java" "${args[@]}" \
      db.monacgraph.app.MonacGraphServer "${2:-gremlin-server-bw.yaml}"
    ;;
  *) echo 'Usage: bash scripts/bwgraph.sh {build|test|start [server.yaml]}' >&2; exit 2 ;;
esac
