package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.Server;

public class StompServer {

    public static void main(String[] args) {

        // Check that at least port and server type were provided
        if (args.length < 2) {
            System.out.println("Usage:");
            System.out.println("java StompServer <port> tpc");
            System.out.println("java StompServer <port> reactor <numThreads>");
            return;
        }

        int port = Integer.parseInt(args[0]);
        String serverType = args[1].toLowerCase();
        // Start a Thread-Per-Client server
        if (serverType.equals("tpc")) {
            Server.threadPerClient(
                    port,
                    () -> new StompMessagingProtocolImpl(),
                    StompMessageEncoderDecoder::new).serve();
        // Start a Reactor server
        } else if (serverType.equals("reactor")) {
            // Reactor requires number of worker threads
            if (args.length < 3) {
                System.out.println("Missing numThreads for reactor");
                return;
            }

            int numThreads = Integer.parseInt(args[2]);

            Server.reactor(
                    numThreads,
                    port,
                    () -> new StompMessagingProtocolImpl(),
                    StompMessageEncoderDecoder::new).serve();

        } else {
            System.out.println("Invalid server type. Use 'tpc' or 'reactor'.");
        }
    }
}