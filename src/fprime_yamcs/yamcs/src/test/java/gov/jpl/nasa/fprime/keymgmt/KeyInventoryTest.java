package gov.jpl.nasa.fprime.keymgmt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import gov.jpl.nasa.fprime.keymgmt.KeyInventory.KeyState;

public class KeyInventoryTest {

    @Test
    public void nominalLifecycle() {
        KeyInventory inventory = new KeyInventory();
        inventory.register(128);
        assertEquals(KeyState.PRE_ACTIVATION, inventory.get(128).getState());
        inventory.transition(128, KeyState.ACTIVE);
        inventory.transition(128, KeyState.DEACTIVATED);
        inventory.transition(128, KeyState.DESTROYED);
        assertEquals(KeyState.DESTROYED, inventory.get(128).getState());
    }

    @Test
    public void preActivationKeyCanBeDestroyed() {
        KeyInventory inventory = new KeyInventory();
        inventory.register(128);
        inventory.transition(128, KeyState.DESTROYED);
        assertEquals(KeyState.DESTROYED, inventory.get(128).getState());
    }

    @Test
    public void illegalTransitionsRejected() {
        KeyInventory inventory = new KeyInventory();
        inventory.register(128);
        assertThrows(IllegalStateException.class, () -> inventory.transition(128, KeyState.DEACTIVATED));
        inventory.transition(128, KeyState.ACTIVE);
        assertThrows(IllegalStateException.class, () -> inventory.transition(128, KeyState.PRE_ACTIVATION));
        assertThrows(IllegalStateException.class, () -> inventory.transition(128, KeyState.DESTROYED));
    }

    @Test
    public void duplicateKeyIdRejectedUntilDestroyed() {
        KeyInventory inventory = new KeyInventory();
        inventory.register(128);
        assertThrows(IllegalStateException.class, () -> inventory.register(128));
        inventory.transition(128, KeyState.DESTROYED);
        inventory.register(128);
        assertEquals(KeyState.PRE_ACTIVATION, inventory.get(128).getState());
    }

    @Test
    public void unknownKeyRejected() {
        KeyInventory inventory = new KeyInventory();
        assertThrows(IllegalStateException.class, () -> inventory.transition(999, KeyState.ACTIVE));
        assertThrows(IllegalStateException.class, () -> inventory.markVerified(999));
    }

    @Test
    public void verificationFlag() {
        KeyInventory inventory = new KeyInventory();
        inventory.register(128);
        assertFalse(inventory.get(128).isVerified());
        inventory.markVerified(128);
        assertTrue(inventory.get(128).isVerified());
    }
}
