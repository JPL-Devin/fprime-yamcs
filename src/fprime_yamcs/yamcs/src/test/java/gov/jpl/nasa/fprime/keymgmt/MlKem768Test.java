package gov.jpl.nasa.fprime.keymgmt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises the OpenSSL subprocess wrapper end-to-end. Requires an
 * ML-KEM-capable OpenSSL (>= 3.5); tests are skipped otherwise. Point
 * KEYMGMT_TEST_OPENSSL at a specific binary to override the PATH lookup.
 */
public class MlKem768Test {

    private static String opensslBinary;
    private static boolean available;

    @BeforeAll
    public static void checkOpenssl() {
        opensslBinary = System.getenv().getOrDefault("KEYMGMT_TEST_OPENSSL", "openssl");
        try {
            new MlKem768(opensslBinary).verifyAvailable();
            available = true;
        } catch (Exception e) {
            available = false;
        }
    }

    @Test
    public void encapsulationRoundTrip(@TempDir Path dir) throws Exception {
        assumeTrue(available, "OpenSSL >= 3.5 not available");
        Path privateKey = dir.resolve("priv.pem");
        Path publicKey = dir.resolve("pub.pem");
        run(opensslBinary, "genpkey", "-algorithm", "ML-KEM-768", "-out", privateKey.toString());
        run(opensslBinary, "pkey", "-in", privateKey.toString(), "-pubout", "-out", publicKey.toString());

        MlKem768.Encapsulation encapsulation = new MlKem768(opensslBinary).encapsulate(publicKey);
        assertEquals(MlKem768.CIPHERTEXT_LENGTH, encapsulation.getCiphertext().length);
        assertEquals(MlKem768.SHARED_SECRET_LENGTH, encapsulation.getSharedSecret().length);

        // Decapsulate with the private key (spacecraft side) and compare secrets
        Path ctFile = dir.resolve("ct.bin");
        Path ssFile = dir.resolve("ss.bin");
        Files.write(ctFile, encapsulation.getCiphertext());
        run(opensslBinary, "pkeyutl", "-decap", "-inkey", privateKey.toString(),
                "-in", ctFile.toString(), "-secret", ssFile.toString());
        assertArrayEquals(encapsulation.getSharedSecret(), Files.readAllBytes(ssFile));
    }

    @Test
    public void missingPublicKeyFails() {
        assumeTrue(available, "OpenSSL >= 3.5 not available");
        MlKem768 mlkem = new MlKem768(opensslBinary);
        assertThrows(IOException.class, () -> mlkem.encapsulate(Path.of("/nonexistent.pem")));
    }

    @Test
    public void missingBinaryFails() {
        MlKem768 mlkem = new MlKem768("/nonexistent/openssl");
        assertThrows(IOException.class, mlkem::verifyAvailable);
    }

    private static void run(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        if (p.waitFor() != 0) {
            throw new IOException("Command failed: " + String.join(" ", cmd) + "\n" + out);
        }
    }
}
