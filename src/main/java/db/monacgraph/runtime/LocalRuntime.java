package db.monacgraph.runtime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

/**
 * Pins JNI temp files, Hugging Face weights and pip caches to {@code .cache/}
 * under the repository so clones can run without setting machine-specific env vars.
 *
 * <p>{@code MONACGRAPH_HOME} and {@code MONACGRAPH_CACHE} still override discovery
 * when a caller already has a layout. {@code MONACGRAPH_KEEP_SYSTEM_TMP=1} leaves
 * {@code java.io.tmpdir} on the OS default.
 */
public final class LocalRuntime {
    public static final String HOME_ENV = "MONACGRAPH_HOME";
    public static final String CACHE_ENV = "MONACGRAPH_CACHE";
    public static final String KEEP_SYSTEM_TMP_ENV = "MONACGRAPH_KEEP_SYSTEM_TMP";

    private static boolean installed;
    private static Path projectRoot;
    private static Path cacheDir;

    private LocalRuntime() {}

    public static synchronized void install() {
        if (installed) {
            return;
        }
        Path cache = cacheDir();
        Path tmp = cache.resolve("tmp");
        try {
            Files.createDirectories(tmp);
            Files.createDirectories(cache.resolve("huggingface"));
            Files.createDirectories(cache.resolve("pip"));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create local cache directories under " + cache, e);
        }
        if (!keepSystemTmp() && shouldRedirectTmp(tmp)) {
            System.setProperty("java.io.tmpdir", tmp.toAbsolutePath().toString());
        }
        installed = true;
    }

    public static Path projectRoot() {
        Path cached = projectRoot;
        if (cached != null) {
            return cached;
        }
        synchronized (LocalRuntime.class) {
            if (projectRoot != null) {
                return projectRoot;
            }
            String home = System.getenv(HOME_ENV);
            if (home != null && !home.isBlank()) {
                Path path = Path.of(home).toAbsolutePath().normalize();
                projectRoot = path;
                return path;
            }
            Path start = Path.of(System.getProperty("user.dir", ".")).toAbsolutePath().normalize();
            for (Path dir = start; dir != null; dir = dir.getParent()) {
                if (isProjectRoot(dir)) {
                    projectRoot = dir;
                    return dir;
                }
            }
            projectRoot = start;
            return start;
        }
    }

    public static Path cacheDir() {
        Path cached = cacheDir;
        if (cached != null) {
            return cached;
        }
        synchronized (LocalRuntime.class) {
            if (cacheDir != null) {
                return cacheDir;
            }
            String override = System.getenv(CACHE_ENV);
            cacheDir = override != null && !override.isBlank()
                    ? Path.of(override).toAbsolutePath().normalize()
                    : projectRoot().resolve(".cache");
            return cacheDir;
        }
    }

    public static Path tmpDir() {
        return cacheDir().resolve("tmp");
    }

    public static void applyTo(ProcessBuilder builder) {
        install();
        Map<String, String> env = builder.environment();
        Path cache = cacheDir().toAbsolutePath();
        Path tmp = tmpDir().toAbsolutePath();
        Path huggingface = cache.resolve("huggingface");
        env.putIfAbsent("MONACGRAPH_CACHE", cache.toString());
        env.putIfAbsent("HF_HOME", huggingface.toString());
        env.putIfAbsent("HUGGINGFACE_HUB_CACHE", huggingface.resolve("hub").toString());
        env.putIfAbsent("TRANSFORMERS_CACHE", huggingface.toString());
        env.putIfAbsent("PIP_CACHE_DIR", cache.resolve("pip").toString());
        if (!keepSystemTmp()) {
            env.put("TMP", tmp.toString());
            env.put("TEMP", tmp.toString());
            env.put("TMPDIR", tmp.toString());
        }
    }

    static boolean isProjectRoot(Path dir) {
        return Files.isRegularFile(dir.resolve("pom.xml"))
                && Files.isDirectory(dir.resolve("src")
                .resolve("main")
                .resolve("java")
                .resolve("db")
                .resolve("monacgraph"));
    }

    static boolean isSystemTmp(Path tmp) {
        return looksLikeSystemTmp(tmp.toString())
                || looksLikeSystemTmp(tmp.toAbsolutePath().normalize().toString());
    }

    private static boolean looksLikeSystemTmp(String raw) {
        String windows = raw.replace('/', '\\').toLowerCase(Locale.ROOT);
        String unix = raw.replace('\\', '/').toLowerCase(Locale.ROOT);
        if (windows.contains("appdata\\local\\temp") || windows.contains("appdata\\local\\tmp")) {
            return true;
        }
        if (unix.equals("/tmp") || unix.startsWith("/tmp/")
                || unix.equals("/var/tmp") || unix.startsWith("/var/tmp/")) {
            return true;
        }
        return unix.startsWith("/private/var/folders/") || unix.contains("/var/folders/");
    }

    private static boolean keepSystemTmp() {
        String value = System.getenv(KEEP_SYSTEM_TMP_ENV);
        return value != null && (value.equals("1") || value.equalsIgnoreCase("true"));
    }

    private static boolean shouldRedirectTmp(Path projectTmp) {
        String current = System.getProperty("java.io.tmpdir");
        if (current == null || current.isBlank()) {
            return true;
        }
        Path tmp = Path.of(current).toAbsolutePath().normalize();
        Path cache = cacheDir().toAbsolutePath().normalize();
        Path ours = projectTmp.toAbsolutePath().normalize();
        if (tmp.equals(ours) || tmp.startsWith(cache)) {
            return false;
        }
        return isSystemTmp(tmp);
    }
}
