package io.strimzi.kafka.bridge.http.generator;

import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.servers.Server;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author ckaratza
 * Tries to interrogate vertx router and build an OpenAPI specification. Tries to interrogate handlers of route with
 * OpenApi Operation methods and cross-reference with route information. By no means all OpenApi 3 spec is covered, so this part will be adjusted
 * based on use cases encountered.
 */
public final class OpenApiSpecGenerator {
    private static final Logger log = LoggerFactory.getLogger(OpenApiSpecGenerator.class);

    public static OpenAPI generateOpenApiSpecFromRouter(Router router, String title, String version, String serverUrl) {
        log.info("Generating Spec for vertx routes.");
        OpenAPI openAPI = new OpenAPI();
        Info info = new Info();
        info.setTitle(title);
        info.setVersion(version);
        Server server = new Server();
        server.setUrl(serverUrl);
        openAPI.servers(Collections.singletonList(server));
        openAPI.setInfo(info);

        Map<String, PathItem> paths = extractAllPaths(router);
        extractOperationInfo(router, paths);
        paths.forEach(openAPI::path);
        return openAPI;
    }

    static private Map<String, PathItem> extractAllPaths(Router router) {
        return router.getRoutes().stream().filter(x -> x.getPath() != null)
                .map(Route::getPath).distinct().collect(Collectors.toMap(x -> x, x -> new PathItem()));
    }

    static private void extractOperationInfo(Router router, Map<String, PathItem> paths) {
        router.getRoutes().forEach(route -> {
            PathItem pathItem = paths.get(route.getPath());
            if (pathItem != null) {
                List<Operation> operations = extractOperations(route, pathItem);
                operations.forEach(operation -> operation.setParameters(extractPathParams(route.getPath())));
            }
        });
        decorateOperationsFromAnnotationsOnHandlers(router, paths);
    }

    private static void decorateOperationsFromAnnotationsOnHandlers(Router router, Map<String, PathItem> paths) {
        router.getRoutes().stream().filter(x -> x.getPath() != null).forEach(route -> {
            try {
                Field contextHandlers = route.getClass().getDeclaredField("contextHandlers");
                contextHandlers.setAccessible(true);
                List<Handler<RoutingContext>> handlers = (List<Handler<RoutingContext>>) contextHandlers.get(route);
                handlers.forEach(handler -> {
                    try {
                        Class<?> delegate = handler.getClass().getDeclaredField("arg$1").getType();
                        Arrays.stream(delegate.getDeclaredMethods()).distinct().forEach(method -> {
                            io.swagger.v3.oas.annotations.Operation annotation = method.getAnnotation(io.swagger.v3.oas.annotations.Operation.class);
                            if (annotation != null ) {
                                String httpMethod = annotation.method();
                                PathItem pathItem = paths.get(route.getPath());
                                Operation matchedOperation = null;
                                switch (PathItem.HttpMethod.valueOf(httpMethod.toUpperCase())) {
                                    case TRACE:
                                        matchedOperation = pathItem.getTrace();
                                        break;
                                    case PUT:
                                        matchedOperation = pathItem.getPut();
                                        break;
                                    case POST:
                                        matchedOperation = pathItem.getPost();
                                        break;
                                    case PATCH:
                                        matchedOperation = pathItem.getPatch();
                                        break;
                                    case GET:
                                        matchedOperation = pathItem.getGet();
                                        break;
                                    case OPTIONS:
                                        matchedOperation = pathItem.getOptions();
                                        break;
                                    case HEAD:
                                        matchedOperation = pathItem.getHead();
                                        break;
                                    case DELETE:
                                        matchedOperation = pathItem.getDelete();
                                        break;
                                    default:
                                        break;
                                }
                                if (matchedOperation != null && (annotation.operationId().equals(route.getPath().substring(1)))) {
                                    AnnotationMappers.decorateOperationFromAnnotation(annotation, matchedOperation);
                                    RequestBody body = method.getParameters()[0].getAnnotation(RequestBody.class);
                                    if (body != null) {
                                        matchedOperation.setRequestBody(AnnotationMappers.fromRequestBody(body));
                                    }
                                }
                            }
                        });
                    } catch (NoSuchFieldException e) {
                        log.warn(e.getMessage());
                    }
                });
            } catch (IllegalAccessException | NoSuchFieldException e) {
                log.warn(e.getMessage());
            }
        });
    }

    private static List<Parameter> extractPathParams(String fullPath) {
        String[] split = fullPath.split("\\/");
        return Arrays.stream(split).filter(x -> x.startsWith(":")).map(x -> {
            Parameter param = new Parameter();
            param.name(x.substring(1));
            return param;
        }).collect(Collectors.toList());
    }

    private static List<Operation> extractOperations(Route route, PathItem pathItem) {
        try {
            Field methods = route.getClass().getDeclaredField("methods");
            methods.setAccessible(true);
            Set<HttpMethod> httpMethods = (Set<HttpMethod>) methods.get(route);
            return httpMethods.stream().map(httpMethod -> {
                Operation operation = new Operation();
                switch (PathItem.HttpMethod.valueOf(httpMethod.name())) {
                    case TRACE:
                        pathItem.trace(operation);
                        break;
                    case PUT:
                        pathItem.put(operation);
                        break;
                    case POST:
                        pathItem.post(operation);
                        break;
                    case PATCH:
                        pathItem.patch(operation);
                        break;
                    case GET:
                        pathItem.get(operation);
                        break;
                    case OPTIONS:
                        pathItem.options(operation);
                        break;
                    case HEAD:
                        pathItem.head(operation);
                        break;
                    case DELETE:
                        pathItem.delete(operation);
                        break;
                    default:
                        break;
                }
                return operation;
            }).collect(Collectors.toList());

        } catch (NoSuchFieldException | IllegalAccessException e) {
            log.warn(e.getMessage());
            return Collections.emptyList();
        }
    }
}
