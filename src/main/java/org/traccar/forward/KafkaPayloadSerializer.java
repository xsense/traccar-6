/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.forward;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.Map;

/**
 * Serializes forwarder payloads for Kafka, embedding string values that hold JSON objects
 * (or arrays of objects) as real JSON instead of escaped strings.
 *
 * Replaces the old regex approach (strip {@code "{"}, {@code "}"} quotes and every backslash),
 * which produced invalid JSON for values such as {@code "[{\"num\":\"sd1\"}]"} and corrupted any
 * string containing escaped characters.
 */
public final class KafkaPayloadSerializer {

    private KafkaPayloadSerializer() {
    }

    public static String serialize(ObjectMapper objectMapper, Object value) throws JsonProcessingException {
        JsonNode tree = objectMapper.readTree(objectMapper.writeValueAsString(value));
        return objectMapper.writeValueAsString(expand(objectMapper, tree));
    }

    private static JsonNode expand(ObjectMapper objectMapper, JsonNode node) {
        if (node.isObject()) {
            ObjectNode object = (ObjectNode) node;
            Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                field.setValue(expand(objectMapper, field.getValue()));
            }
        } else if (node.isArray()) {
            ArrayNode array = (ArrayNode) node;
            for (int i = 0; i < array.size(); i++) {
                array.set(i, expand(objectMapper, array.get(i)));
            }
        } else if (node.isTextual()) {
            JsonNode parsed = parseEmbedded(objectMapper, node.textValue());
            if (parsed != null) {
                return expand(objectMapper, parsed);
            }
        }
        return node;
    }

    private static JsonNode parseEmbedded(ObjectMapper objectMapper, String text) {
        String trimmed = text.trim();
        boolean object = trimmed.startsWith("{") && trimmed.endsWith("}");
        boolean arrayOfObjects = trimmed.startsWith("[") && trimmed.endsWith("]") && trimmed.contains("{");
        if (!object && !arrayOfObjects) {
            return null;
        }
        try {
            JsonNode parsed = objectMapper.readTree(trimmed);
            return parsed != null && parsed.isContainerNode() ? parsed : null;
        } catch (JsonProcessingException e) {
            return null;
        }
    }

}
