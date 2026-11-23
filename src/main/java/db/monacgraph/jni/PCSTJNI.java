
package db.monacgraph.jni;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * JNI bridge for PCSTFast.
 *
 * C++ implementation:
 * pcst/src/pcst_jni.cpp
 */
public class PCSTJNI {

    private static final boolean AVAILABLE;

    static {

        boolean loaded = false;

        try {

            String osName = System.getProperty("os.name")
                    .toLowerCase();

            String resourcePath;
            String extension;

            if (osName.contains("win")) {

                resourcePath = "storage/windows/pcst.dll";
                extension = ".dll";

            } else if (osName.contains("mac")) {

                resourcePath = "storage/macos/libpcst.dylib";
                extension = ".dylib";

            } else {

                resourcePath = "storage/linux/libpcst.so";
                extension = ".so";
            }

            try (InputStream in =
                    PCSTJNI.class.getClassLoader()
                            .getResourceAsStream(resourcePath)) {

                if (in == null) {

                    throw new IOException(
                        "Native PCST library not found: "
                        + resourcePath
                    );
                }

                File tempFile = File.createTempFile(
                    "libpcst",
                    extension
                );

                tempFile.deleteOnExit();

                Files.copy(
                    in,
                    tempFile.toPath(),
                    StandardCopyOption.REPLACE_EXISTING
                );

                System.load(tempFile.getAbsolutePath());

                loaded = true;
            }

        } catch (Exception | LinkageError e) {

            System.err.println(
                "[PCSTJNI] Failed to load native library: "
                + e.getMessage()
            );
        }

        AVAILABLE = loaded;
    }


    /**
     * Check whether the native PCST library is available.
     */
    public static boolean isAvailable() {
        return AVAILABLE;
    }


    /**
     * Execute PCST.
     *
     * @param vertexCount number of vertices
     * @param edgeU source endpoint indices
     * @param edgeV destination endpoint indices
     * @param prizes vertex prizes
     * @param costs edge costs
     *
     * @return int[][]:
     *         result[0] = selected vertex indices
     *         result[1] = selected edge indices
     */
    public int[][] solve(
            int vertexCount,
            int[] edgeU,
            int[] edgeV,
            double[] prizes,
            double[] costs) {

        if (!AVAILABLE) {

            throw new UnsupportedOperationException(
                "PCST native library is unavailable."
            );
        }

        return nativeSolve(
            vertexCount,
            edgeU,
            edgeV,
            prizes,
            costs
        );
    }


    /**
     * Actual JNI method.
     */
    private native int[][] nativeSolve(
            int vertexCount,
            int[] edgeU,
            int[] edgeV,
            double[] prizes,
            double[] costs
    );
}

