package db.monacgraph.jni;

import db.monacgraph.so.SubgraphPhiCallback;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * JNI bridge to the C++ subgraph matching engine (libsubgraph_matching).
 *
 * <p>The native library is optional. If it is not found in the classpath,
 * subgraph matching is silently disabled and {@link #isAvailable()} returns
 * {@code false}. No exception is thrown at startup.
 *
 * <p>Expected native implementation: {@code subgraph_jni.cpp}.
 * Build with:
 * <pre>
 *   g++ -std=c++17 -O3 -shared -fPIC \
 *       -I$JAVA_HOME/include -I$JAVA_HOME/include/linux \
 *       -Iinclude \
 *       subgraph_jni.cpp -o libsubgraph_matching.so
 * </pre>
 */
public class SubgraphJNI {

    /** True only if the native library was loaded successfully at startup. */
    private static final boolean AVAILABLE;

    static {
        boolean loaded = false;
        try {
            String osName = System.getProperty("os.name").toLowerCase();
            String resourcePath;
            String libExtension;

            if (osName.contains("win")) {
                resourcePath = "storage/windows/subgraph_matching.dll";
                libExtension = ".dll";
            } else if (osName.contains("mac")) {
                resourcePath = "storage/macos/libsubgraph_matching.dylib";
                libExtension = ".dylib";
            } else {
                resourcePath = "storage/linux/libsubgraph_matching.so";
                libExtension = ".so";
            }

            InputStream in = SubgraphJNI.class.getClassLoader().getResourceAsStream(resourcePath);
            if (in == null) {
                // Native library not bundled — subgraph feature is simply unavailable
                System.out.println("[SubgraphJNI] Native library not found (" + resourcePath
                        + "). Subgraph matching is disabled.");
            } else {
                File tempFile = File.createTempFile("libsubgraph_matching", libExtension);
                tempFile.deleteOnExit();
                Files.copy(in, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                in.close();
                System.load(tempFile.getAbsolutePath());
                loaded = true;
            }
        } catch (Exception e) {
            // Any unexpected error during loading also disables the feature gracefully
            System.out.println("[SubgraphJNI] Failed to load native library: "
                    + e.getMessage() + ". Subgraph matching is disabled.");
        }
        AVAILABLE = loaded;
    }

    /**
     * Returns whether the native subgraph matching library was loaded successfully.
     * If {@code false}, calling {@link #runSubgraphMatch} will throw
     * {@link UnsupportedOperationException}.
     */
    public static boolean isAvailable() {
        return AVAILABLE;
    }

    /**
     * Runs the full subgraph matching pipeline (filter → order → backtrack)
     * with second-order logic constraints evaluated via {@code phiCallback}.
     *
     * <p>The C++ side:
     * <ol>
     *   <li>Reads both graph files.</li>
     *   <li>Runs GraphQL candidate filtering and RI-DS ordering.</li>
     *   <li>Runs backtracking; for each candidate tuple the DecisionTree
     *       calls {@code phiCallback.evaluate(int[] coords)} via JNI.</li>
     *   <li>Collects all matches that pass the second-order formula.</li>
     * </ol>
     *
     * @param dataGraphPath  path to {@code transfer/data.graph}
     * @param queryGraphPath path to {@code transfer/query.graph}
     * @param quantifiers    int[] of length k; {@code 0} = FORALL, {@code 1} = EXIST
     * @param phiCallback    Java object whose {@code evaluate([I)Z} method is
     *                       invoked by C++ for every candidate coordinate tuple
     * @return flat long array:
     *         <pre>
     *           result[0]       = M (total match count)
     *           result[1..M*qn] = M consecutive blocks of qn data-vertex indices
     *         </pre>
     * @throws UnsupportedOperationException if the native library is not available
     */
    public long[] runSubgraphMatch(String dataGraphPath,
                                   String queryGraphPath,
                                   int[]  quantifiers,
                                   SubgraphPhiCallback phiCallback) {
        if (!AVAILABLE) {
            throw new UnsupportedOperationException(
                    "Subgraph matching is not available: native library was not loaded. " +
                            "Build the C++ subgraph engine and place the library under " +
                            "resources/storage/<os>/subgraph_matching.<ext>.");
        }
        return nativeRunSubgraphMatch(dataGraphPath, queryGraphPath, quantifiers, phiCallback);
    }

    /** Actual JNI call, only invoked after availability is confirmed. */
    private native long[] nativeRunSubgraphMatch(String dataGraphPath,
                                                 String queryGraphPath,
                                                 int[]  quantifiers,
                                                 SubgraphPhiCallback phiCallback);
}