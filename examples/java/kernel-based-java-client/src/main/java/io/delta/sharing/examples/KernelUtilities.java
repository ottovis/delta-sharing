package io.delta.sharing.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.UncheckedIOException;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultJsonRow;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.types.TableSchemaSerDe;

public class KernelUtilities
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Utility method to deserialize a {@link Row} object from the JSON form.
     */
    public static Row deserializeRowFromJson(TableClient tableClient, String jsonRowWithSchema)
    {
        try {
            JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonRowWithSchema);
            JsonNode schemaNode = jsonNode.get("schema");
            StructType schema =
                TableSchemaSerDe.fromJson(tableClient.getJsonHandler(), schemaNode.asText());
            return new DefaultJsonRow((ObjectNode) jsonNode.get("row"), schema);
        }
        catch (JsonProcessingException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
