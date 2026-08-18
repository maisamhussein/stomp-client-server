package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
// REMOVED: import bgu.spl.net.api.MessagingProtocol;
import bgu.spl.net.api.StompMessagingProtocol; // CHANGED: use STOMP-specific protocol

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;

public class BlockingConnectionHandler<T> implements Runnable, ConnectionHandler<T> {
    // CHANGED: MessagingProtocol<T> -> StompMessagingProtocol<T>
    private final StompMessagingProtocol<T> protocol;
    private final MessageEncoderDecoder<T> encdec;
    private final Socket sock;
    private BufferedInputStream in;
    private BufferedOutputStream out;
    private volatile boolean connected = true;
    // ADDED: shared Connections object and unique connectionId
    // Reason: needed for send/disconnect operations between different clients
    private final Connections<T> connections;
    private final int connectionId;

    // CHANGED: constructor now receives connections and connectionId
    public BlockingConnectionHandler(Socket sock, MessageEncoderDecoder<T> reader, StompMessagingProtocol<T> protocol,
            Connections<T> connections, int connectionId) {
        this.sock = sock;
        this.encdec = reader;
        this.protocol = protocol;
        this.connections = connections;
        this.connectionId = connectionId;
    }

    @Override
    public void run() {
        try (Socket sock = this.sock) { // just for automatic closing
            int read;

            in = new BufferedInputStream(sock.getInputStream());
            out = new BufferedOutputStream(sock.getOutputStream());
            // ADDED: protocol initialization before processing messages
           // Reason: STOMP protocol needs connectionId and connections to be set
            protocol.start(connectionId, connections);
            while (!protocol.shouldTerminate() && connected && (read = in.read()) >= 0) {
                T nextMessage = encdec.decodeNextByte((byte) read);
                if (nextMessage != null) {
                    // CHANGED: process is void (no response returned)
                    protocol.process(nextMessage);
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            connected = false;

            // ADDED: ensure connection is removed on exit
            // Reason: cleanup server-side state when client disconnects
           connections.disconnect(connectionId);
        }
    }

    @Override
    public void close() throws IOException {
        connected = false;
        sock.close();
    }

    @Override
    public void send(T msg) {
       // ADDED: implementation required by the assignment
        // Used by ConnectionsImpl.send(...) to push messages to this client
        if (msg == null || !connected || out == null)
            return;

        try {
            byte[] bytes = encdec.encode(msg);
            synchronized (this) {
                out.write(bytes);
                out.flush();
            }
        } catch (IOException e) {
            connected = false;
        }

    }
}