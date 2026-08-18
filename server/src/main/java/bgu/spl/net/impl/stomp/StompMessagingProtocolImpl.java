package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;
import bgu.spl.net.srv.ConnectionsImpl;
import bgu.spl.net.impl.data.Database;
import bgu.spl.net.impl.data.LoginStatus;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
public class StompMessagingProtocolImpl implements StompMessagingProtocol<String> {
    // Global message-id counter for MESSAGE frames (shared across all protocol instances)
    private static final AtomicInteger messageIdGen = new AtomicInteger(1);
    // This connection's id (given by the server/handler)
    private int connectionId;
    // Shared server connections object (used to send/broadcast frames and disconnect)
    private Connections<String> connections;
    // When true -> handler loop stops and connection will be closed
    private boolean shouldTerminate = false;
    // True after successful CONNECT
    private boolean loggedIn = false;
    // Logged-in username for this connection (null if not logged in)
    private String username = null;
    
    @Override
    public void start(int connectionId, Connections<String> connections) {
        // Save connection state so later we can send frames and disconnect
        this.connectionId = connectionId;
        this.connections = connections;
    }

    @Override
    public void process(String message) {
        // Main dispatcher: parse STOMP frame and route to the correct handler
        if (message == null) return;

        Frame f = Frame.parse(message);
        String cmd = f.command;

        switch (cmd) {
            case "CONNECT":
                handleConnect(f);
                break;

            case "SUBSCRIBE":
                // Reject if client is not logged in
                requireLoginOrError(f);
                if (!shouldTerminate) handleSubscribe(f);
                break;

            case "UNSUBSCRIBE":
                requireLoginOrError(f);
                if (!shouldTerminate) handleUnsubscribe(f);
                break;

            case "SEND":
                requireLoginOrError(f);
                if (!shouldTerminate) handleSend(f);
                break;

            case "DISCONNECT":
                requireLoginOrError(f);
                if (!shouldTerminate) handleDisconnect(f);
                break;

            default:
                // Unknown command -> ERROR and close connection
                sendErrorAndClose(f, "malformed frame received", "Unknown command: " + cmd);
        }
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }

    private void handleConnect(Frame f) {
        // Validates CONNECT headers and logs the user in using Database
        String login = f.headers.get("login");
        String pass = f.headers.get("passcode");
        String version = f.headers.get("accept-version");

        // Missing required headers -> ERROR
        if (login == null || pass == null || version == null) {
            sendErrorAndClose(f, "malformed frame received",
                    "CONNECT must include accept-version, login, passcode");
            return;
        }

        // We only accept STOMP 1.2
        if (!version.contains("1.2")) {
            sendErrorAndClose(f, "malformed frame received",
                    "Client must support STOMP 1.2");
            return;
        }

        // Ask Database to authenticate / register and check if user already connected
        LoginStatus st = Database.getInstance().login(connectionId, login, pass);

        switch (st) {
            case ADDED_NEW_USER:
            case LOGGED_IN_SUCCESSFULLY:
                // Mark as logged in and reply CONNECTED
                loggedIn = true;
                username = login;
                sendToMe("CONNECTED\nversion:1.2\n\n");
                return;

            case ALREADY_LOGGED_IN:
                sendErrorAndClose(f, "malformed frame received",
                        "User already logged in: " + login);
                return;

            case WRONG_PASSWORD:
                sendErrorAndClose(f, "Wrong password",
                        "Wrong passcode for user: " + login);
                return;

            case CLIENT_ALREADY_CONNECTED:
                sendErrorAndClose(f, "malformed frame received",
                        "This connection is already connected");
                return;

            default:
                sendErrorAndClose(f, "server error",
                        "Unknown login status: " + st);
        }
    }

    private void handleSubscribe(Frame f) {
        // Adds this connection as a subscriber to the destination with a given subscription id
        String destination = f.headers.get("destination");
        String subId = f.headers.get("id");

        if (destination == null || subId == null) {
            sendErrorAndClose(f, "malformed frame received",
                    "SUBSCRIBE must include destination and id");
            return;
        }

        ConnectionsImpl<String> impl = asImplOrError(f);
        if (impl == null) return;

        impl.subscribe(connectionId, destination, subId);

        // If receipt header exists -> send RECEIPT back
        sendReceiptIfRequested(f);
    }

    private void handleUnsubscribe(Frame f) {
        // Removes a subscription by its id (STOMP UNSUBSCRIBE works by subscription-id)
        String subId = f.headers.get("id");
        if (subId == null) {
            sendErrorAndClose(f, "malformed frame received",
                    "UNSUBSCRIBE must include id");
            return;
        }

        ConnectionsImpl<String> impl = asImplOrError(f);
        if (impl == null) return;

        impl.unsubscribe(connectionId, subId);
        sendReceiptIfRequested(f);
    }

    private void handleSend(Frame f) {
        // Broadcasts a MESSAGE frame to all subscribers of the destination
        String destination = f.headers.get("destination");
        if (destination == null) {
            sendErrorAndClose(f, "malformed frame received",
                    "SEND did not contain a destination header, which is REQUIRED");
            return;
        }

        ConnectionsImpl<String> impl = asImplOrError(f);
        if (impl == null) return;

        // Enforce rule: only subscribers can SEND to a destination
        if (!impl.isSubscribed(connectionId, destination)) {
            sendErrorAndClose(f, "malformed frame received",
                    "Client is not subscribed to: " + destination);
            return;
        }

        // Optional: if SEND includes filename, track upload in Database
        String filename = f.headers.get("filename");
        if (filename != null && loggedIn && username != null) {
            Database.getInstance().trackFileUpload(username, filename, destination);
        }

        int msgId = messageIdGen.getAndIncrement();

        // For each subscriber: build MESSAGE frame with subscription id + message-id + destination + body
        Set<Integer> subscribers = impl.getChannelSubscriptions().get(destination);
        if (subscribers != null) {
            for (Integer cid : new HashSet<>(subscribers)) {
                String subId = impl.getSubscriptionId(cid, destination);
                if (subId == null) continue;

                String body = (f.body == null) ? "" : f.body;

                String stompMsg =
                        "MESSAGE\n" +
                        "subscription:" + subId + "\n" +
                        "message-id:" + msgId + "\n" +
                        "destination:" + destination + "\n\n" +
                        body;

                sendTo(cid, stompMsg);
            }
        }

        sendReceiptIfRequested(f);
    }

    private void handleDisconnect(Frame f) {
        // DISCONNECT requires receipt header -> send RECEIPT and then close
        String receipt = f.headers.get("receipt");
        if (receipt == null) {
            sendErrorAndClose(f, "malformed frame received",
                    "DISCONNECT must include receipt header");
            return;
        }

        sendToMe("RECEIPT\nreceipt-id:" + receipt + "\n\n");
        cleanupAndClose();
    }

    private void requireLoginOrError(Frame f) {
        // Helper: blocks any command except CONNECT before login
        if (!loggedIn) {
            sendErrorAndClose(f, "malformed frame received",
                    "Not connected. Please CONNECT first.");
        }
    }

    private void sendReceiptIfRequested(Frame f) {
        // Helper: if a frame has "receipt" header, respond with RECEIPT
        String receipt = f.headers.get("receipt");
        if (receipt != null) {
            sendToMe("RECEIPT\nreceipt-id:" + receipt + "\n\n");
        }
    }

    private void sendErrorAndClose(Frame related, String shortMsg, String details) {
        // Builds an ERROR frame (includes receipt-id if the original had receipt) and closes connection
        String receipt = related.headers.get("receipt");

        StringBuilder sb = new StringBuilder();
        sb.append("ERROR\n");
        if (receipt != null)
            sb.append("receipt-id:").append(receipt).append("\n");
        sb.append("message: ").append(shortMsg).append("\n\n");
        sb.append(details);

        sendToMe(sb.toString());
        cleanupAndClose();
    }

    private void cleanupAndClose() {
        // Cleanup server-side state and mark this protocol as terminated
        if (!shouldTerminate) {
            if (loggedIn) {
                Database.getInstance().logout(connectionId);
                loggedIn = false;
                username = null;
            }
            connections.disconnect(connectionId);
            shouldTerminate = true;
        }
    }

    private ConnectionsImpl<String> asImplOrError(Frame f) {
        // We expect ConnectionsImpl so we can use subscribe/unsubscribe helpers
        if (!(connections instanceof ConnectionsImpl)) {
            sendErrorAndClose(f, "server error", "Connections is not ConnectionsImpl");
            return null;
        }
        return (ConnectionsImpl<String>) connections;
    }

    // Sends a frame to this client (adds the null terminator before sending)
    private void sendToMe(String frameWithoutNull) {
        sendTo(connectionId, frameWithoutNull);
    }

    // Sends a frame to a specific connectionId (adds the null terminator before sending)
    private void sendTo(int cid, String frameWithoutNull) {
        String out = ensureNullTerminated(frameWithoutNull);
        connections.send(cid, out);
    }

    // Ensures every outgoing frame ends with '\0' as STOMP requires
    private static String ensureNullTerminated(String s) {
        if (s == null) return "\u0000";
        return s.endsWith("\u0000") ? s : (s + "\u0000");
    }

    // Removes trailing '\0' if it exists (helps parsing when input already includes it)
    private static String stripTrailingNull(String s) {
        if (s == null) return null;
        return s.endsWith("\u0000") ? s.substring(0, s.length() - 1) : s;
    }

    private static class Frame {
        // Parsed command (first line of the frame)
        final String command;

        // Parsed headers: key -> value
        final Map<String, String> headers;

        // Frame body (everything after the empty line)
        final String body;

        private Frame(String command, Map<String, String> headers, String body) {
            this.command = command;
            this.headers = headers;
            this.body = body;
        }

        static Frame parse(String raw) {
            // Basic STOMP parsing: normalize newlines, strip trailing null, split headers/body
            String s = raw.replace("\r\n", "\n");
            s = stripTrailingNull(s);

            String[] parts = s.split("\n\n", 2);
            String head = parts[0];
            String body = (parts.length > 1) ? parts[1] : "";

            String[] lines = head.split("\n");
            String cmd = (lines.length > 0) ? lines[0].trim() : "";

            Map<String, String> headers = new HashMap<>();
            for (int i = 1; i < lines.length; i++) {
                String line = lines[i];
                int idx = line.indexOf(':');
                if (idx > 0) {
                    String k = line.substring(0, idx).trim();
                    String v = line.substring(idx + 1).trim();
                    headers.put(k, v);
                }
            }

            return new Frame(cmd, headers, body);
        }
    }
}