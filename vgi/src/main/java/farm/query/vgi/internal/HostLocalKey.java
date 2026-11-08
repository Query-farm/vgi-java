// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.Set;

/**
 * A 32-byte key shared by every worker process one user runs on this host, for encrypting
 * secret ATTACH options on transports that do not seal opaque data (stdio, unix sockets)
 * when no signing key is configured.
 *
 * <p>Those transports are host-local by definition, but a pooled launcher or the subprocess
 * transport may route one attach's calls to several processes, so a per-process key would
 * leave the others unable to open the values. The key lives in
 * {@code $XDG_RUNTIME_DIR/vgi} (else {@code <per-user temp dir>/vgi-<user>}, the temp dir being
 * {@code $TMPDIR} when set, else {@code java.io.tmpdir}), a directory
 * created {@code 0700}, in a file created {@code 0600}; both must be owned by the current
 * user. Created atomically (write a temporary file, then hard-link it into place), so
 * processes starting together agree on one key. {@code VGI_ATTACH_SECRET_KEY_FILE} names a
 * different file.
 */
public final class HostLocalKey {

    /** Environment variable naming the key file explicitly. */
    public static final String KEY_FILE_ENV = "VGI_ATTACH_SECRET_KEY_FILE";

    private static volatile byte[] cached;

    private HostLocalKey() {}

    /**
     * The host-local key, created on first use.
     *
     * @return 32 key bytes
     * @throws IllegalStateException when the key file cannot be created or read safely
     */
    public static byte[] get() {
        byte[] k = cached;
        if (k != null) return k;
        synchronized (HostLocalKey.class) {
            if (cached == null) cached = load(keyFile());
            return cached;
        }
    }

    static Path keyFile() {
        String explicit = System.getenv(KEY_FILE_ENV);
        if (explicit != null && !explicit.isBlank()) return Path.of(explicit);
        String runtime = System.getenv("XDG_RUNTIME_DIR");
        Path dir = runtime != null && !runtime.isBlank() && Files.isDirectory(Path.of(runtime))
                ? Path.of(runtime, "vgi")
                : Path.of(tempDir(), "vgi-" + System.getProperty("user.name", "user"));
        return dir.resolve("attach-secret-options.key");
    }

    /** The per-user temp dir: {@code $TMPDIR} (what other SDKs read), else {@code java.io.tmpdir}. */
    static String tempDir() {
        String t = System.getenv("TMPDIR");
        return t != null && !t.isBlank() ? t : System.getProperty("java.io.tmpdir");
    }

    static byte[] load(Path file) {
        try {
            Path dir = file.toAbsolutePath().getParent();
            boolean posix = dir.getFileSystem().supportedFileAttributeViews().contains("posix");
            if (!Files.isDirectory(dir)) {
                try {
                    if (posix) {
                        Files.createDirectories(dir, PosixFilePermissions.asFileAttribute(
                                PosixFilePermissions.fromString("rwx------")));
                    } else {
                        Files.createDirectories(dir);
                    }
                } catch (FileAlreadyExistsException ignored) {
                    // created concurrently
                }
            }
            if (posix) requireOwnedPrivate(dir, Set.of(PosixFilePermission.GROUP_WRITE, PosixFilePermission.OTHERS_WRITE));
            if (!Files.exists(file, LinkOption.NOFOLLOW_LINKS)) {
                byte[] fresh = new byte[32];
                new SecureRandom().nextBytes(fresh);
                Path tmp = posix
                        ? Files.createTempFile(dir, ".attach-key", ".tmp",
                                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
                        : Files.createTempFile(dir, ".attach-key", ".tmp");
                try {
                    Files.write(tmp, fresh);
                    try {
                        Files.createLink(file, tmp);
                    } catch (FileAlreadyExistsException lost) {
                        // another process won the race: use its key
                    }
                } finally {
                    Files.deleteIfExists(tmp);
                }
            }
            if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalStateException("attach secret key " + file + " is not a regular file");
            }
            if (posix) {
                requireOwnedPrivate(file, Set.of(PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE,
                        PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE));
            }
            byte[] key = Files.readAllBytes(file);
            if (key.length != 32) {
                throw new IllegalStateException("attach secret key " + file + " must hold exactly 32 bytes, has "
                        + key.length + " (set VGI_SIGNING_KEY, or point " + KEY_FILE_ENV + " at a valid key file)");
            }
            return key;
        } catch (IOException e) {
            throw new IllegalStateException("cannot create the attach secret key " + file
                    + " (set VGI_SIGNING_KEY, or " + KEY_FILE_ENV + "): " + e.getMessage(), e);
        }
    }

    private static void requireOwnedPrivate(Path p, Set<PosixFilePermission> forbidden) throws IOException {
        String me = System.getProperty("user.name");
        String owner = Files.getOwner(p, LinkOption.NOFOLLOW_LINKS).getName();
        if (me != null && !me.equals(owner)) {
            throw new IllegalStateException(p + " is owned by " + owner + ", not " + me
                    + " (set VGI_SIGNING_KEY, or " + KEY_FILE_ENV + ")");
        }
        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(p, LinkOption.NOFOLLOW_LINKS);
        for (PosixFilePermission f : forbidden) {
            if (perms.contains(f)) throw new IllegalStateException(p + " is accessible to other users ("
                    + PosixFilePermissions.toString(perms) + "; set VGI_SIGNING_KEY, or " + KEY_FILE_ENV + ")");
        }
    }
}
