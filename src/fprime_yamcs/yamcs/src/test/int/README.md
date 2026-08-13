# Key Management Integration Test

End-to-end test of the optional SDLS key management service
(`gov.jpl.nasa.fprime.keymgmt.KeyManagementService`) against a running
YAMCS with an SDLS-encrypted TC link. Requires OpenSSL >= 3.5 and the
Python `cryptography` package.

## Setup

1. Generate a test ML-KEM-768 key pair and initial SDLS key:

   ```bash
   openssl genpkey -algorithm ML-KEM-768 -out mlkem768-priv.pem
   openssl pkey -in mlkem768-priv.pem -pubout -out src/main/yamcs/etc/mlkem768-pub.pem
   head -c 32 /dev/urandom > src/main/yamcs/etc/sdls-key.bin
   ```

2. In `yamcs.fprime-project.yaml`, enable the service (see the README
   "SDLS Key Management" section) with `sdlsTargets: [{link: UDP_TC_OUT, spi: 1}]`,
   and configure SDLS on the TC link:

   ```yaml
   - name: UDP_TC_OUT
     # ...
     encryption:
       - spi: 1
         class: org.yamcs.security.sdls.SecurityAssociationAes256Gcm128Factory
         args:
           keyFile: etc/sdls-key.bin
     virtualChannels:
       - vcId: 1
         encryptionSpi: 1
         # ...
   ```

3. Start YAMCS (`mvn yamcs:run`), then run the fake spacecraft:

   ```bash
   python3 keymgmt_fake_spacecraft.py mlkem768-priv.pem src/main/yamcs/etc/sdls-key.bin
   ```

4. Trigger two rekeys (UI button at `http://localhost:8090/keymgmt/` or
   `curl -X POST http://localhost:8090/keymgmt/api/rekey`, twice).

Expected output ends with `SDLS INTEGRATION TEST PASS`: the script
decrypts the rekey frames with the initial key, decapsulates the KEM
ciphertext, recovers the session key from the OTAR PDU, and confirms
post-rekey frames are encrypted with the new session key only.
