package org.traccar.forward;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.traccar.MainModule;
import org.traccar.model.Device;
import org.traccar.model.Position;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaPayloadSerializerTest {

    private final ObjectMapper objectMapper = MainModule.provideObjectMapper();

    private JsonNode serialize(Position position) throws Exception {
        Device device = new Device();
        device.setId(2997);
        PositionData positionData = new PositionData();
        positionData.setPosition(position);
        positionData.setDevice(device);
        // must be parseable JSON
        return objectMapper.readTree(KafkaPayloadSerializer.serialize(objectMapper, positionData));
    }

    @Test
    public void testHowenArrayDetail() throws Exception {
        Position position = new Position("howen");
        position.set("alarmDetail", "[{\"num\":\"sd1\",\"ow\":5}]");
        position.set("alarm", "diskDetection");

        JsonNode attributes = serialize(position).get("position").get("attributes");
        JsonNode detail = attributes.get("alarmDetail");
        assertTrue(detail.isArray());
        assertEquals("sd1", detail.get(0).get("num").asText());
        assertEquals(5, detail.get(0).get("ow").asInt());
        assertEquals("diskDetection", attributes.get("alarm").asText());
    }

    @Test
    public void testObjectDetailEmbedded() throws Exception {
        Position position = new Position("howen");
        position.set("alarmDetail", "{\"sp\":\"80\",\"tp\":1}");

        JsonNode detail = serialize(position).get("position").get("attributes").get("alarmDetail");
        assertTrue(detail.isObject());
        assertEquals("80", detail.get("sp").asText());
    }

    @Test
    public void testPlainStringsUntouched() throws Exception {
        Position position = new Position("howen");
        position.set("text", "a \"quoted\" back\\slash {x}");
        position.set("list", "[1,2,3]");
        position.set("broken", "{not json}");

        JsonNode attributes = serialize(position).get("position").get("attributes");
        assertEquals("a \"quoted\" back\\slash {x}", attributes.get("text").asText());
        assertEquals("[1,2,3]", attributes.get("list").asText());
        assertEquals("{not json}", attributes.get("broken").asText());
    }

}
