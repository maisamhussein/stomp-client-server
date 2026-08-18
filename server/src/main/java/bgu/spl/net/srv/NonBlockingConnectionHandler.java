package bgu.spl.net.srv;
import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.StompMessagingProtocol;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NonBlockingConnectionHandler<T> implements ConnectionHandler<T> {
    private static final int BUFFER_ALLOCATION_SIZE = 1 << 13; // 8k
    private static final ConcurrentLinkedQueue<ByteBuffer> BUFFER_POOL = new ConcurrentLinkedQueue<>();
    // CHANGED: MessagingProtocol<T> -> StompMessagingProtocol<T>
    private final StompMessagingProtocol<T> protocol;
    private final MessageEncoderDecoder<T> encdec;
    private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
    private final SocketChannel chan;
    private final Reactor reactor;
    private final Connections<T> connections;// ADDED: connections + id
    private final int connectionId;
    private volatile boolean started = false;// ADDED: ensure start happens once and before process

    public NonBlockingConnectionHandler(MessageEncoderDecoder<T> reader, StompMessagingProtocol<T> protocol,
            SocketChannel chan, Reactor reactor, Connections<T> connections, int connectionId) {
        this.chan = chan;
        this.encdec = reader;
        this.protocol = protocol;
        this.reactor = reactor;
        this.connections = connections;
        this.connectionId = connectionId;
    }

    public Runnable continueRead() {
        ByteBuffer buf = leaseBuffer();
        boolean success = false;
        try {
            success = chan.read(buf) != -1;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        if (success) {
            buf.flip();
            return () -> {
                try {
                    // ADDED: start inside worker task (not selector thread)
                    synchronized (this) {
                        if (!started) {
                            started = true;
                            protocol.start(connectionId, connections);
                        }
                    }
                    while (buf.hasRemaining()) {
                        T nextMessage = encdec.decodeNextByte(buf.get());
                        if (nextMessage != null) {
                            // CHANGED: process is void
                            protocol.process(nextMessage);
                        }
                    }
                } finally {
                    releaseBuffer(buf);
                }
            };
        } else {
            releaseBuffer(buf);
            connections.disconnect(connectionId);
            close();
            return null;
        }
    }

    public void close() {
        try {
            chan.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public boolean isClosed() {
        return !chan.isOpen();
    }

    public void continueWrite() {
        while (!writeQueue.isEmpty()) {
            try {
                ByteBuffer top = writeQueue.peek();
                chan.write(top);
                if (top.hasRemaining())
                    return;
                writeQueue.remove();
            } catch (IOException ex) {
                ex.printStackTrace();
                connections.disconnect(connectionId);
                close();
                return;
            }
        }
        if (writeQueue.isEmpty()) {
            if (protocol.shouldTerminate()) {
                connections.disconnect(connectionId);
                close();
            } else {
                reactor.updateInterestedOps(chan, SelectionKey.OP_READ);
            }
        }
    }

    private static ByteBuffer leaseBuffer() {
        ByteBuffer buff = BUFFER_POOL.poll();
        if (buff == null) {
            return ByteBuffer.allocateDirect(BUFFER_ALLOCATION_SIZE);
        }

        buff.clear();
        return buff;
    }

    private static void releaseBuffer(ByteBuffer buff) {
        BUFFER_POOL.add(buff);
    }

    @Override
    public void send(T msg) {
        // ADDED: required by assignment, used by ConnectionsImpl.send(...)
        if (msg == null)
            return;
        writeQueue.add(ByteBuffer.wrap(encdec.encode(msg)));
        reactor.updateInterestedOps(chan, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }
}