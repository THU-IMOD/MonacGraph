package db.monacgraph.runtime;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalRuntimeTest {

    @Test
    void projectRootFindsMavenModule() {
        Path root = LocalRuntime.projectRoot();
        assertTrue(Files.isRegularFile(root.resolve("pom.xml")));
        assertTrue(LocalRuntime.isProjectRoot(root));
    }

    @Test
    void cacheDirDefaultsUnderProjectUnlessOverridden() {
        Path cache = LocalRuntime.cacheDir();
        String override = System.getenv(LocalRuntime.CACHE_ENV);
        if (override == null || override.isBlank()) {
            assertEquals(LocalRuntime.projectRoot().resolve(".cache"), cache);
        } else {
            assertEquals(Path.of(override).toAbsolutePath().normalize(), cache);
        }
    }

    @Test
    void recognizesOsTempDirectories() {
        assertTrue(LocalRuntime.isSystemTmp(Path.of("C:\\Users\\demo\\AppData\\Local\\Temp")));
        assertTrue(LocalRuntime.isSystemTmp(Path.of("/tmp")));
        assertTrue(LocalRuntime.isSystemTmp(Path.of("/tmp/jni-extract")));
        assertTrue(LocalRuntime.isSystemTmp(Path.of("/var/folders/xx/yyyy/T")));
        assertFalse(LocalRuntime.isSystemTmp(Path.of("/data/project/.cache/tmp")));
    }

    @Test
    void applyToPinsChildProcessCaches() {
        LocalRuntime.install();
        ProcessBuilder builder = new ProcessBuilder("java", "-version");
        LocalRuntime.applyTo(builder);
        Map<String, String> env = builder.environment();
        assertFalse(env.getOrDefault("HF_HOME", "").isBlank());
        assertFalse(env.getOrDefault("PIP_CACHE_DIR", "").isBlank());
        if (!keepSystemTmp()) {
            assertEquals(LocalRuntime.tmpDir().toAbsolutePath().toString(), env.get("TMPDIR"));
        }
    }

    private static boolean keepSystemTmp() {
        String value = System.getenv(LocalRuntime.KEEP_SYSTEM_TMP_ENV);
        return value != null && (value.equals("1") || value.equalsIgnoreCase("true"));
    }
}
