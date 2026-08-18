package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.StompMessagingProtocol;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.function.Supplier;

public abstract class BaseServer<T> implements Server<T> {

    private final int port;

    // Changed to StompMessagingProtocol because our server works specifically with STOMP.
    // We need STOMP-specific behavior (like start(connectionId, connections)),
    // which does not exist in the generic MessagingProtocol interface.
    private final Supplier<StompMessagingProtocol<T>> protocolFactory;

    private final Supplier<MessageEncoderDecoder<T>> encdecFactory;
    private ServerSocket sock;

    public BaseServer(int port,
            Supplier<StompMessagingProtocol<T>> protocolFactory,
            Supplier<MessageEncoderDecoder<T>> encdecFactory) {
        this.port = port;
        this.protocolFactory = protocolFactory;
        this.encdecFactory = encdecFactory;
        this.sock = null;
    }

    @Override
    public void serve() {
        // Added a single shared ConnectionsImpl instance.
        // This is needed because STOMP supports subscriptions and message broadcasting,so all clients must be managed from one shared place.
        ConnectionsImpl<T> connections = ConnectionsImpl.getInstance();

        try (ServerSocket serverSock = new ServerSocket(port)) {
            System.out.println("Server started");
            this.sock = serverSock;

            while (!Thread.currentThread().isInterrupted()) {
                Socket clientSock = serverSock.accept();
                // ADDED: unique id per connection , Each client gets a unique connectionId.
                int id = ConnectionsImpl.nextId();
                // CHANGED: handler gets connections + id
                // The handler receives the shared connections object and its connectionId.
                // This allows the protocol to send messages to other clients if needed.
                BlockingConnectionHandler<T> handler = new BlockingConnectionHandler<>(
                        clientSock,
                        encdecFactory.get(),
                        protocolFactory.get(),
                        connections,
                        id);
                connections.addConnection(id, handler);

                execute(handler);
            }
        } catch (IOException ex) {
            // optional
            // ex.printStackTrace();
        }

        System.out.println("server closed!!!");
    }

    @Override
    public void close() throws IOException {
        if (sock != null)
            sock.close();
    }

    protected abstract void execute(BlockingConnectionHandler<T> handler);
}