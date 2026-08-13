package gov.jpl.nasa.fprime.keymgmt;

import org.yamcs.http.ForbiddenException;
import org.yamcs.http.HandlerContext;
import org.yamcs.http.HttpHandler;
import org.yamcs.http.NotFoundException;
import org.yamcs.security.SystemPrivilege;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * HTTP interface for {@link KeyManagementService}, mounted at
 * {@code /keymgmt}:
 *
 * <ul>
 *   <li>{@code GET  /keymgmt/}          — operator UI page
 *   <li>{@code POST /keymgmt/api/rekey}     — trigger a session rekey
 *   <li>{@code GET  /keymgmt/api/inventory} — key inventory with states
 *   <li>{@code GET  /keymgmt/api/status}    — last rekey outcome
 * </ul>
 */
public class KeyManagementHandler extends HttpHandler {

    private final KeyManagementService service;

    public KeyManagementHandler(KeyManagementService service) {
        this.service = service;
    }

    @Override
    public boolean requireAuth() {
        return true;
    }

    @Override
    public void handle(HandlerContext ctx) {
        // Key operations affect the command link; gate on link-control privilege
        if (!ctx.getUser().hasSystemPrivilege(SystemPrivilege.ControlLinks)) {
            throw new ForbiddenException("ControlLinks privilege required");
        }

        String path = ctx.getPathWithoutContext();
        switch (path) {
        case "/keymgmt":
        case "/keymgmt/":
            ctx.requireGET();
            ctx.sendResource("/keymgmt/index.html");
            return;
        case "/keymgmt/api/rekey":
            ctx.requirePOST();
            KeyManagementService.RekeyStatus result = service.rekey();
            ctx.sendOK(statusJson(result));
            return;
        case "/keymgmt/api/inventory":
            ctx.requireGET();
            ctx.sendOK(inventoryJson());
            return;
        case "/keymgmt/api/status":
            ctx.requireGET();
            ctx.sendOK(statusJson(service.getLastStatus()));
            return;
        default:
            throw new NotFoundException();
        }
    }

    private JsonObject inventoryJson() {
        JsonObject root = new JsonObject();
        JsonArray keys = new JsonArray();
        for (KeyInventory.KeyRecord record : service.getInventory().list()) {
            JsonObject key = new JsonObject();
            key.addProperty("keyId", record.getKeyId());
            key.addProperty("state", record.getState().name());
            key.addProperty("verified", record.isVerified());
            key.addProperty("lastTransitionMillis", record.getLastTransitionMillis());
            keys.add(key);
        }
        root.add("keys", keys);
        return root;
    }

    private static JsonObject statusJson(KeyManagementService.RekeyStatus status) {
        JsonObject json = new JsonObject();
        if (status == null) {
            json.addProperty("state", "none");
            return json;
        }
        json.addProperty("state", status.success ? "success" : "failure");
        json.addProperty("sessionKeyId", status.sessionKeyId);
        json.addProperty("message", status.message);
        json.addProperty("timestampMillis", status.timestampMillis);
        return json;
    }
}
