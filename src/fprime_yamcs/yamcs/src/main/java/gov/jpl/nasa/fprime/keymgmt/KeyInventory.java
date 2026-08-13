package gov.jpl.nasa.fprime.keymgmt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the CCSDS 355.1-B-1 lifecycle state of each managed session key:
 * Pre-Activation, Active, Deactivated, Destroyed (5.4.1.2). Only state and
 * metadata are kept here — never key material.
 */
public class KeyInventory {

    public enum KeyState {
        PRE_ACTIVATION,
        ACTIVE,
        DEACTIVATED,
        DESTROYED;
    }

    /** Metadata for one managed key. */
    public static final class KeyRecord {
        private final int keyId;
        private volatile KeyState state;
        private volatile long lastTransitionMillis;
        private volatile boolean verified;

        KeyRecord(int keyId, KeyState state) {
            this.keyId = keyId;
            this.state = state;
            this.lastTransitionMillis = System.currentTimeMillis();
        }

        public int getKeyId() {
            return keyId;
        }

        public KeyState getState() {
            return state;
        }

        public long getLastTransitionMillis() {
            return lastTransitionMillis;
        }

        public boolean isVerified() {
            return verified;
        }
    }

    private final Map<Integer, KeyRecord> records = new ConcurrentHashMap<>();

    /** Register a freshly uploaded key in Pre-Activation state. */
    public synchronized KeyRecord register(int keyId) {
        KeyRecord existing = records.get(keyId);
        if (existing != null && existing.state != KeyState.DESTROYED) {
            throw new IllegalStateException(
                    "Key ID " + keyId + " already in use (state " + existing.state + ")");
        }
        KeyRecord record = new KeyRecord(keyId, KeyState.PRE_ACTIVATION);
        records.put(keyId, record);
        return record;
    }

    /** Transition a key, enforcing the lifecycle's legal transitions. */
    public synchronized void transition(int keyId, KeyState to) {
        KeyRecord record = records.get(keyId);
        if (record == null) {
            throw new IllegalStateException("Unknown key ID " + keyId);
        }
        if (!isLegal(record.state, to)) {
            throw new IllegalStateException(
                    "Illegal key transition " + record.state + " -> " + to + " for key " + keyId);
        }
        record.state = to;
        record.lastTransitionMillis = System.currentTimeMillis();
    }

    public synchronized void markVerified(int keyId) {
        KeyRecord record = records.get(keyId);
        if (record == null) {
            throw new IllegalStateException("Unknown key ID " + keyId);
        }
        record.verified = true;
    }

    public KeyRecord get(int keyId) {
        return records.get(keyId);
    }

    public List<KeyRecord> list() {
        return new ArrayList<>(records.values());
    }

    private static boolean isLegal(KeyState from, KeyState to) {
        switch (from) {
        case PRE_ACTIVATION:
            return to == KeyState.ACTIVE || to == KeyState.DESTROYED;
        case ACTIVE:
            return to == KeyState.DEACTIVATED;
        case DEACTIVATED:
            return to == KeyState.DESTROYED;
        default:
            return false;
        }
    }
}
