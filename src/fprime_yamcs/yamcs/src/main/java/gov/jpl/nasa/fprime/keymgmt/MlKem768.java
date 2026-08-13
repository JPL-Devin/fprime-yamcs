package gov.jpl.nasa.fprime.keymgmt;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ML-KEM-768 (FIPS 203) encapsulation via the OpenSSL command line tool.
 *
 * <p>Requires OpenSSL &gt;= 3.5, the first release with built-in ML-KEM
 * support. The binary path is configurable so deployments can point at a
 * locally built OpenSSL while the system copy remains older.
 */
public class MlKem768 {

    /** ML-KEM-768 ciphertext size in bytes (FIPS 203). */
    public static final int CIPHERTEXT_LENGTH = 1088;
    /** ML-KEM shared secret size in bytes (FIPS 203). */
    public static final int SHARED_SECRET_LENGTH = 32;

    private static final Pattern VERSION_RE = Pattern.compile("OpenSSL (\\d+)\\.(\\d+)");
    private static final long SUBPROCESS_TIMEOUT_S = 30;

    private final String opensslBinary;

    /** Result of one encapsulation: uplink the ciphertext, keep the secret. */
    public static final class Encapsulation {
        private final byte[] ciphertext;
        private final byte[] sharedSecret;

        Encapsulation(byte[] ciphertext, byte[] sharedSecret) {
            this.ciphertext = ciphertext;
            this.sharedSecret = sharedSecret;
        }

        public byte[] getCiphertext() {
            return ciphertext;
        }

        public byte[] getSharedSecret() {
            return sharedSecret;
        }

        /** Zeroize the shared secret once it is no longer needed. */
        public void destroy() {
            Arrays.fill(sharedSecret, (byte) 0);
        }
    }

    public MlKem768(String opensslBinary) {
        this.opensslBinary = opensslBinary;
    }

    /**
     * Verify the configured binary exists and supports ML-KEM (OpenSSL >= 3.5).
     *
     * @throws IOException if the binary is missing or too old
     */
    public void verifyAvailable() throws IOException {
        String out = run(new String[] { opensslBinary, "version" }, null);
        Matcher m = VERSION_RE.matcher(out);
        if (!m.find()) {
            throw new IOException("Cannot parse OpenSSL version from: " + out.trim());
        }
        int major = Integer.parseInt(m.group(1));
        int minor = Integer.parseInt(m.group(2));
        if (major < 3 || (major == 3 && minor < 5)) {
            throw new IOException("OpenSSL >= 3.5 required for ML-KEM-768, found: " + out.trim());
        }
    }

    /**
     * Encapsulate against the given ML-KEM-768 public key (PEM).
     *
     * @param publicKeyPem path to the spacecraft public key
     * @return ciphertext (to uplink) and shared secret (transaction KEK)
     */
    public Encapsulation encapsulate(Path publicKeyPem) throws IOException {
        if (!Files.isReadable(publicKeyPem)) {
            throw new IOException("Public key file not readable: " + publicKeyPem);
        }
        Path dir = Files.createTempDirectory("mlkem");
        Path ctFile = dir.resolve("ct.bin");
        Path ssFile = dir.resolve("ss.bin");
        try {
            run(new String[] {
                    opensslBinary, "pkeyutl", "-encap",
                    "-inkey", publicKeyPem.toString(), "-pubin",
                    "-out", ctFile.toString(),
                    "-secret", ssFile.toString(),
            }, null);
            byte[] ciphertext = Files.readAllBytes(ctFile);
            byte[] sharedSecret = Files.readAllBytes(ssFile);
            if (ciphertext.length != CIPHERTEXT_LENGTH) {
                throw new IOException("Unexpected ML-KEM-768 ciphertext length: " + ciphertext.length
                        + " (is the key really ML-KEM-768?)");
            }
            if (sharedSecret.length != SHARED_SECRET_LENGTH) {
                throw new IOException("Unexpected shared secret length: " + sharedSecret.length);
            }
            return new Encapsulation(ciphertext, sharedSecret);
        } finally {
            shred(ssFile);
            Files.deleteIfExists(ctFile);
            Files.deleteIfExists(ssFile);
            Files.deleteIfExists(dir);
        }
    }

    /** Overwrite file contents before deletion so the secret does not linger on disk. */
    private static void shred(Path file) throws IOException {
        if (Files.exists(file)) {
            Files.write(file, new byte[(int) Files.size(file)]);
        }
    }

    private String run(String[] cmd, byte[] stdin) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        try {
            if (stdin != null) {
                p.getOutputStream().write(stdin);
            }
            p.getOutputStream().close();
            String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!p.waitFor(SUBPROCESS_TIMEOUT_S, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                throw new IOException("OpenSSL timed out: " + String.join(" ", cmd));
            }
            if (p.exitValue() != 0) {
                throw new IOException("OpenSSL failed (exit " + p.exitValue() + "): " + output.trim());
            }
            return output;
        } catch (InterruptedException e) {
            p.destroyForcibly();
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while running OpenSSL", e);
        }
    }
}
