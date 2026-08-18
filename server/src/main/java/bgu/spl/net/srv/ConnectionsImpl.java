package bgu.spl.net.srv;

import java.util.Map;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionsImpl<T> implements Connections<T> {
    // Maps connectionId to its ConnectionHandler (only active connections)
    private final ConcurrentHashMap<Integer, ConnectionHandler<T>> connectionsById = new ConcurrentHashMap<>();
    // Maps a channel (destination) to the set of connectionIds subscribed to it
    private final ConcurrentHashMap<String, Set<Integer>> channelSubscriptions = new ConcurrentHashMap<>();
    // For each connectionId, stores a map: subscriptionId -> channel
    // Used to support UNSUBSCRIBE which works by subscription-id
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, String>> subscriptionsByConnection = new ConcurrentHashMap<>();
    // Generates unique ids for new client connections
    private static final AtomicInteger ID_GEN = new AtomicInteger(0);
    // Singleton instance – one shared ConnectionsImpl for the whole server
    private static final ConnectionsImpl<?> instance = new ConnectionsImpl<>();

    private ConnectionsImpl() {}

    @Override
    public boolean send(int connectionId, T msg) {
        // Get the handler responsible for this connection
        ConnectionHandler<T> handler = connectionsById.get(connectionId);
        // If the client is connected, send the message
        if (handler != null) {
            handler.send(msg);
            return true;
        }
        // Client not found / not connected
        return false;
    }

    @Override
    public void send(String channel, T msg) {
        // Get all subscribers of the channel
        Set<Integer> channelSubscribers = channelSubscriptions.get(channel);

        // If the channel exists, send the message to each subscriber
        if (channelSubscribers != null) {
            for (Integer id : new HashSet<>(channelSubscribers)) {
                send(id, msg);
            }
        }
    }

    @Override
    public void disconnect(int connectionId) {
        connectionsById.remove(connectionId);
        // remove the client from all channels
        channelSubscriptions.forEach((channel, subscribers) -> subscribers.remove(connectionId));
        // remove empty channels
        channelSubscriptions.entrySet().removeIf(e -> e.getValue().isEmpty());
        subscriptionsByConnection.remove(connectionId);
    }

    public void addConnection(int connectionId, ConnectionHandler<T> handler) {
        connectionsById.put(connectionId, handler);
    }

    public void subscribe(int connectionId, String channel, String subscriptionId) {
        channelSubscriptions.computeIfAbsent(channel, c -> ConcurrentHashMap.newKeySet()).add(connectionId);
        subscriptionsByConnection.computeIfAbsent(connectionId, id -> new ConcurrentHashMap<>()).put(subscriptionId,
                channel);
    }

    public void unsubscribe(int connectionId, String subscriptionId) {
        ConcurrentHashMap<String, String> map = subscriptionsByConnection.get(connectionId);
        if (map == null)
            return;
        String channel = map.remove(subscriptionId);
        if (channel == null)
            return;
        Set<Integer> set = channelSubscriptions.get(channel);
        if (set != null) {
            set.remove(connectionId);
            if (set.isEmpty()) {
                channelSubscriptions.remove(channel, set);
            }
        }
        // if no subscriptions left for this connection
        if (map.isEmpty()) {
            subscriptionsByConnection.remove(connectionId, map);
        }
    }

    // returns true iff this connection has a subscription to 'channel'
    public boolean isSubscribed(int connectionId, String channel) {
        ConcurrentHashMap<String, String> subs = subscriptionsByConnection.get(connectionId);
        if (subs == null)
            return false;
        return subs.containsValue(channel);
    }

    public String getSubscriptionId(int connectionId, String channel) {
        ConcurrentHashMap<String, String> subs = subscriptionsByConnection.get(connectionId);
        if (subs == null)
            return null;

        for (Map.Entry<String, String> entry : subs.entrySet()) { // subscriptionId -> channel
            if (channel.equals(entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }


    public ConcurrentHashMap<Integer, ConnectionHandler<T>> getConnectionsById() {
        return connectionsById;
    }

    public ConcurrentHashMap<String, Set<Integer>> getChannelSubscriptions() {
        return channelSubscriptions;
    }

    public ConcurrentHashMap<Integer, ConcurrentHashMap<String, String>> getSubscriptionsByConnection() {
        return subscriptionsByConnection;
    }

    public static <T> ConnectionsImpl<T> getInstance() {
        return (ConnectionsImpl<T>) instance;
    }

    public static int nextId() {
        return ID_GEN.incrementAndGet();
    }

}