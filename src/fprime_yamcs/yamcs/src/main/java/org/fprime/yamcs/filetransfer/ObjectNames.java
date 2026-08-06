package org.fprime.yamcs.filetransfer;

/**
 * Bucket object-name utilities shared by the downlink handlers.
 */
final class ObjectNames {

    private ObjectNames() {
    }

    /**
     * Turn a wire-supplied destination path into a bucket object key:
     * strip the leading '/', and reject empty or '..'-bearing paths so a
     * corrupt or malicious transfer cannot address objects outside the
     * bucket namespace (or, via a local mirror, outside the mirror
     * directory).
     *
     * @throws IllegalArgumentException if the path is empty or contains an
     *         empty, '.' or '..' segment
     */
    static String sanitize(String destinationPath) {
        String name = destinationPath.startsWith("/")
                ? destinationPath.substring(1)
                : destinationPath;
        if (name.isEmpty()) {
            throw new IllegalArgumentException("empty destination path");
        }
        for (String segment : name.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException(
                        "path contains empty, '.' or '..' segment: " + destinationPath);
            }
        }
        return name;
    }
}
