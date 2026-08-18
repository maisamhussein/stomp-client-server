package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.MessageEncoderDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class StompMessageEncoderDecoder implements MessageEncoderDecoder<String> {

    // Buffer that collects incoming bytes until a full STOMP frame is received
    private final List<Byte> buffer = new ArrayList<>();

    @Override
    public String decodeNextByte(byte nextByte) {
        // A STOMP frame is terminated by a null character ('\0')
        if (nextByte == '\u0000') {
            // Convert the accumulated bytes into a UTF-8 string
            byte[] bytes = new byte[buffer.size()];
            for (int i = 0; i < buffer.size(); i++) {
                bytes[i] = buffer.get(i);
            }
            buffer.clear(); // Prepare for the next frame
            return new String(bytes, StandardCharsets.UTF_8);
        }

        // Keep accumulating bytes until the frame terminator is reached
        buffer.add(nextByte);
        return null;
    }

    @Override
    public byte[] encode(String message) {
        // Converts a STOMP frame string into bytes using UTF-8
        if (message == null)
            return new byte[0];

        // The null terminator is handled by the protocol logic,
        // so it is not added here
        return message.getBytes(StandardCharsets.UTF_8);
    }
}