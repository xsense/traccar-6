package org.traccar.protocol;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.traccar.NetworkMessage;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MeitrackSensorSnTest extends ProtocolTest {

    private static final String AAA_PREFIX = "$$[241,868998030372235,AAA,35,13.904936,100.529891,261029073448,A,6,15,0,0,"
            + "1.6,49,17030,3570803,520|3|2337|028CDC97,0600,0000|0000|0000|0187|0962,00000001,0|0|36|0|0|0|0|0,108,0000,";
    private static final String AAA_SUFFIX = ",3,0,,0|0000|0000|0000|0000|0000,ACC|126|186,BUF|0|8192*B0";

    private String outbound(EmbeddedChannel channel) {
        Object message = channel.readOutbound();
        return message instanceof NetworkMessage networkMessage ? networkMessage.getMessage().toString() : null;
    }

    @Test
    public void testSensorSn() throws Exception {

        var decoder = inject(new MeitrackProtocolDecoder(null));
        var channel = new EmbeddedChannel();

        // first AAA on connection requests SN
        Position position = (Position) decoder.decode(channel, null, buffer(AAA_PREFIX + "015500" + AAA_SUFFIX));
        assertFalse(position.hasAttribute(MeitrackProtocolDecoder.KEY_TEMP_SN));
        String request = outbound(channel);
        assertTrue(request.matches("@@[A-Za-z]25,868998030372235,C42\\*[0-9A-F]{2}\r\n"), request);

        // C42 reply with registered sensor
        position = (Position) decoder.decode(channel, null, binary(
                "24244133352c3836383939383033303337323233352c4334322c28ecbb8110000069012a32420d0a"));
        assertEquals("28ecbb8110000069", position.getString(MeitrackProtocolDecoder.KEY_TEMP_SN));
        assertNull(outbound(channel)); // already registered, no C40

        // following AAA carries SN and temperature, no new request within interval
        position = (Position) decoder.decode(channel, null, buffer(AAA_PREFIX + "015500" + AAA_SUFFIX));
        assertEquals("28ecbb8110000069", position.getString(MeitrackProtocolDecoder.KEY_TEMP_SN));
        assertEquals(85.0, position.getDouble(Position.PREFIX_TEMP + 1));
        assertNull(outbound(channel));

        // temperature disappears, sensor disconnected
        position = (Position) decoder.decode(channel, null, buffer(AAA_PREFIX + AAA_SUFFIX));
        assertEquals("", position.getString(MeitrackProtocolDecoder.KEY_TEMP_SN));
        assertNull(outbound(channel));

        // temperature appears again, SN requested immediately
        position = (Position) decoder.decode(channel, null, buffer(AAA_PREFIX + "015500" + AAA_SUFFIX));
        assertEquals("", position.getString(MeitrackProtocolDecoder.KEY_TEMP_SN));
        assertTrue(outbound(channel).contains(",C42*"));

    }

    @Test
    public void testRegistrationRetry() throws Exception {

        var decoder = inject(new MeitrackProtocolDecoder(null));
        var channel = new EmbeddedChannel();

        // number 01 already used by a disconnected sensor, retry with 02
        Position position = (Position) decoder.decode(channel, null, binary(
                "24244a33362c3836383939383033303337323233352c4334302c28e1c8811000000001022a43450d0a"));
        assertEquals("Temp1:28e1c88110000000:2", position.getString(Position.KEY_RESULT));
        Object message = channel.readOutbound();
        String hex = io.netty.buffer.ByteBufUtil.hexDump((io.netty.buffer.ByteBuf) ((NetworkMessage) message).getMessage());
        assertTrue(hex.contains("4334302c28e1c88110000000022a"), hex);

        // number limit reached, no retry
        decoder.decode(channel, null, binary(
                "24244a33362c3836383939383033303337323233352c4334302c28e1c8811000000008022a43450d0a"));
        assertNull(channel.readOutbound());

    }

}
