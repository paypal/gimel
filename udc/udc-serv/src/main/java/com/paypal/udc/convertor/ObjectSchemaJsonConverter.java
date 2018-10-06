package com.paypal.udc.convertor;

import java.io.IOException;
import java.util.List;
import javax.persistence.AttributeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paypal.udc.entity.objectschema.Schema;


public class ObjectSchemaJsonConverter implements AttributeConverter<List<Schema>, String> {

    final static Logger logger = LoggerFactory.getLogger(ObjectSchemaJsonConverter.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(final List<Schema> hostProperties) {
        try {
            return this.objectMapper.writeValueAsString(hostProperties);
        }
        catch (final JsonProcessingException e) {
            logger.error("coudln't convert HostProperties Dto to json, wont persist");
            return null;
        }
    }

    @Override
    public List<Schema> convertToEntityAttribute(final String data) {

        try {
            if (data != null) {
                return this.objectMapper.readValue(data,
                        this.objectMapper.getTypeFactory().constructCollectionType(List.class, Schema.class));
            }
        }
        catch (final IOException e) {
            logger.error("couldn't convert json of properties to Dto", e);
        }
        return null;
    }
}
