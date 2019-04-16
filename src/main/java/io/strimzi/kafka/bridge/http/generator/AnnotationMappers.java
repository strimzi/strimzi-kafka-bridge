package io.strimzi.kafka.bridge.http.generator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.models.headers.Header;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author ckaratza
 * Simple OpenApi Annotation mapping to OpenApi models. This part needs modifications to cover all spec.
 */
final class AnnotationMappers {

    private static final Logger log = LoggerFactory.getLogger(AnnotationMappers.class);

    static void decorateOperationFromAnnotation(Operation annotation, io.swagger.v3.oas.models.Operation operation) {
        operation.summary(annotation.summary());
        operation.description(annotation.description());
        operation.operationId(annotation.operationId());
        operation.deprecated(annotation.deprecated());
        operation.setTags(Arrays.asList(annotation.tags()));

        if(annotation.requestBody().content().length !=0) {
            io.swagger.v3.oas.models.parameters.RequestBody rb = new io.swagger.v3.oas.models.parameters.RequestBody();

            Map<String, Object> map = new HashMap<String, Object>();
            Field[] fields = annotation.requestBody().content()[0].schema().implementation().getDeclaredFields();

            for (Field field : fields) {
                mapParameters(field, map);
            }

            ObjectMapper pojoMapper = new ObjectMapper();
            pojoMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
            Object example = new Object();

            try {
                    example = pojoMapper.readValue(annotation.requestBody().content()[0].schema().example(),
                    annotation.requestBody().content()[0].schema().implementation());

            } catch (IOException e) {
                log.error("The example could not be mapped for operation" + operation.getDescription());
            }

            fields = FieldUtils.getFieldsListWithAnnotation(annotation.requestBody().content()[0].schema().implementation(), Required.class).toArray(new Field[0]);
            List<String> requiredParameters = new ArrayList<String>();

            for(Field requiredField : fields) {
              requiredParameters.add(requiredField.getName());
            }

            Schema model = new Schema();

            model.setType(annotation.requestBody().content()[0].schema().type());
            model.setTitle(annotation.requestBody().content()[0].schema().title());
            model.setProperties(map);
            model.required(requiredParameters);
            model.setExample(example);

            Content cont = new Content()
                .addMediaType("application/json", new MediaType().schema(model));

            rb.setContent(cont);


            rb.setRequired(annotation.requestBody().required());
            rb.description(annotation.requestBody().description());
            operation.requestBody(rb);
        }

        ApiResponses apiResponses = new ApiResponses();
        apiResponses.putAll(
            Arrays.stream(annotation.responses()).map(response -> {
                ApiResponse apiResponse = new ApiResponse();
                apiResponse.description(response.description());
                if (response.content().length > 0) {
                    Arrays.stream(response.content()).forEach(content -> {
                        Content c = getContent(content);
                        apiResponse.content(c);
                    });
                }
                Arrays.stream(response.headers()).forEach(header -> {
                    Header h = new Header();
                    h.description(header.description());
                    h.deprecated(header.deprecated());
                    //h.allowEmptyValue(header.allowEmptyValue());
                    //Optional<Schema> schemaFromAnnotation = AnnotationsUtils.getSchemaFromAnnotation(header.schema());
                    //schemaFromAnnotation.ifPresent(h::schema);
                    h.required(header.required());
                    apiResponse.addHeaderObject(header.name(), h);
                });
                return new ImmutablePair<>(response.responseCode(), apiResponse);
            }).collect(Collectors.toMap(x -> x.left, x -> x.right)));
        operation.responses(apiResponses);
        Arrays.stream(annotation.parameters()).forEach(parameter -> {
            Parameter p = findAlreadyProcessedParamFromVertxRoute(parameter.name(), operation.getParameters());
            if (p == null) {
                p = new Parameter();
                operation.addParametersItem(p);
            }
            p.name(parameter.name());
            p.description(parameter.description());
            p.allowEmptyValue(parameter.allowEmptyValue());
            try {
                p.style(Parameter.StyleEnum.valueOf(parameter.style().name()));
            } catch (IllegalArgumentException ie) {
                log.warn(ie.getMessage());
            }
            p.setRequired(parameter.required());
            p.in(parameter.in().name().toLowerCase());

            /*Optional<Schema> schemaFromAnnotation = AnnotationsUtils.getSchemaFromAnnotation(parameter.schema(),null);
            schemaFromAnnotation.ifPresent(p::schema);*/
            Schema schema = new Schema();
            io.swagger.v3.oas.annotations.media.Schema s = parameter.schema();
            if (!s.ref().isEmpty()) schema.set$ref(s.ref());
            schema.setDeprecated(s.deprecated());
            schema.setDescription(s.description());
            schema.setName(s.name());
            schema.setType(s.type());
            schema.setFormat(s.format());
            p.schema(schema);
        });
    }

    private static void mapParameters(Field field, Map<String, Object> map) {
        Class type = field.getType();
        Class componentType = field.getType().getComponentType();

        Field[] fields = field.getType().getDeclaredFields();
        if (fields.length == 0 && !isPrimitiveOrWrapper(componentType)) {
            //this may be an array
            fields = componentType.getDeclaredFields();
        }

        if (isPrimitiveOrWrapper(type)) {
            map.put(field.getName(), new Schema().type(field.getType().getSimpleName()));
        } else {
            HashMap<String, Object> subMap = new HashMap<String, Object>();
            subMap.put("type", "array");

            if(isPrimitiveOrWrapper(componentType)){
                HashMap<String, Object> arrayMap = new HashMap<String, Object>();
                arrayMap.put("type", componentType.getSimpleName() + "[]");
                subMap.put("type", arrayMap);
            } else {
                subMap.put("$ref", "#/components/schemas/" + componentType.getSimpleName());
            }

            map.put(field.getName(), subMap);
        }
    }

    private  static Boolean isPrimitiveOrWrapper(Type type){
        return type.equals(Double.class) ||
            type.equals(Float.class) ||
            type.equals(Long.class) ||
            type.equals(Integer.class) ||
            type.equals(Short.class) ||
            type.equals(Character.class) ||
            type.equals(Byte.class) ||
            type.equals(Boolean.class) ||
            type.equals(String.class);
    }

    private static Object clean(final String in) {
        return in;
    }

    private static Content getContent(io.swagger.v3.oas.annotations.media.Content content) {

        Map<String, Object> map = new HashMap<String, Object>();
        Field[] fields = content.schema().implementation().getDeclaredFields();

        for (Field field : fields) {
            mapParameters(field, map);
        }

        ObjectMapper pojoMapper = new ObjectMapper();
        pojoMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        Object example = new Object();

        try {
            example = pojoMapper.readValue(content.schema().example(),
                Object.class);
        } catch (IOException e) {
            log.error("The example could not be mapped");
        }

        Schema model = new Schema();

        model.setType(content.schema().type());
        model.setProperties(map);
        model.setExample(example);

        Content cont = new Content()
            .addMediaType("application/json", new MediaType().schema(model));

        return cont;
    }

    private static Parameter findAlreadyProcessedParamFromVertxRoute(final String name, List<Parameter> parameters) {
        for (Parameter parameter : parameters) {
            if (name.equals(parameter.getName()))
                return parameter;
        }
        return null;
    }

    static io.swagger.v3.oas.models.parameters.RequestBody fromRequestBody(RequestBody body) {
        io.swagger.v3.oas.models.parameters.RequestBody rb = new io.swagger.v3.oas.models.parameters.RequestBody();
        rb.setDescription(body.description());
        if (body.content().length == 1) {
            Content c = getContent(body.content()[0]);
            io.swagger.v3.oas.annotations.media.Content content = body.content()[0];
            if (!Void.class.equals(content.array().schema().implementation()))
                c.get(content.mediaType()).getSchema().setExample(clean(content.array().schema().example()));
            else if (!Void.class.equals(content.schema().implementation()))
                c.get(content.mediaType()).getSchema().setExample(content.schema().example());
            rb.setContent(c);
        }
        return rb;
    }
}
