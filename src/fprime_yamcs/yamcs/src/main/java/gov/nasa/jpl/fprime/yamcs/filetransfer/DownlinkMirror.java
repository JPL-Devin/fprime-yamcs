package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes downlinked files into a local mirror directory, containing every
 * write within the mirror root. Containment is checked against real
 * (symlink-resolved) paths, so a symlink planted inside the mirror tree
 * cannot redirect a write outside it. Mirror failures are logged, never
 * thrown: the bucket remains the authoritative store.
 */
final class DownlinkMirror {

    private static final Logger LOG = LoggerFactory.getLogger(DownlinkMirror.class);

    private DownlinkMirror() {
    }

    /**
     * Mirror {@code content} to {@code objectName} under {@code mirrorDir}.
     * No-op when {@code mirrorDir} is null.
     */
    static void write(Path mirrorDir, String objectName, byte[] content) {
        if (mirrorDir == null) {
            return;
        }
        try {
            Path root = Files.createDirectories(mirrorDir).toRealPath();
            Path mirrorPath = root.resolve(objectName).normalize();
            if (!mirrorPath.startsWith(root)) {
                LOG.warn("Refusing to mirror {} outside {}", objectName, root);
                return;
            }
            Files.createDirectories(mirrorPath.getParent());
            // Re-check with symlinks resolved: a link planted within the
            // mirror tree must not redirect the write outside the root.
            Path realParent = mirrorPath.getParent().toRealPath();
            if (!realParent.startsWith(root)) {
                LOG.warn("Refusing to mirror {}: parent resolves outside {}", objectName, root);
                return;
            }
            Path target = realParent.resolve(mirrorPath.getFileName());
            if (Files.isSymbolicLink(target)) {
                LOG.warn("Refusing to mirror {}: target is a symbolic link", objectName);
                return;
            }
            Files.write(target, content);
            LOG.info("Mirrored downlink file to {}", target);
        } catch (IOException e) {
            LOG.warn("Failed to mirror file to {}: {}", mirrorDir, e.getMessage());
        }
    }
}
