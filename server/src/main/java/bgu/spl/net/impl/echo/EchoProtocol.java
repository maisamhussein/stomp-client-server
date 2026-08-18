/*package bgu.spl.net.impl.echo;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;

public class EchoProtocol implements StompMessagingProtocol<String> {

    private int connectionId;
    private Connections<String> connections;
    private boolean shouldTerminate = false;

    @Override
    public void start(int connectionId, Connections<String> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
    }

    @Override
    public void process(String message) {
        if (message == null)
            return;
        // Echo back to the same client
        connections.send(connectionId, message);
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }
}
*/