package com.paypal.udc.convertor;

import java.io.IOException;
import java.util.List;
import javax.persistence.AttributeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class ListStringJsonConverter implements AttributeConverter<List<String>, String> {

    final static Logger logger = LoggerFactory.getLogger(ListStringJsonConverter.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(final List<String> hostProperties) {
        try {
            return this.objectMapper.writeValueAsString(hostProperties);
        }
        catch (final JsonProcessingException e) {
            logger.error("coudln't convert HostProperties Dto to json, wont persist");
            return null;
        }
    }

    @Override
    public List<String> convertToEntityAttribute(final String data) {

        try {
            if (data != null) {
                return this.objectMapper.readValue(data,
                        this.objectMapper.getTypeFactory().constructCollectionType(List.class, String.class));
            }
        }
        catch (final IOException e) {
            logger.error("couldn't convert json of properties to Dto", e);
        }
        return null;
    }
}
