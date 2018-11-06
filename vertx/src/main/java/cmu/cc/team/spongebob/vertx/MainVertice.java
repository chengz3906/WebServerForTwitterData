package cmu.cc.team.spongebob.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;


public class MainVerticle extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

//    private QRCodeParser parser;
//    private KeyValueLRUCache cache;


    @Override
    public void start(Future<Void> startFuture) throws Exception {
//        parser = new QRCodeParser();
//        cache = KeyValueLRUCache.getInstance();
        Future<Void> steps = startHttpServer();
        startFuture.complete();
    }

    private Future<Void> startHttpServer() {
        Future<Void> future = Future.future();
        HttpServer server = vertx.createHttpServer();

        Router router = Router.router(vertx);
        router.get("/").handler(this::indexHandler);
        router.get("/q1").handler(this::qrcodeHandler);

        server
                .requestHandler(router::accept)
                .listen(80, ar -> {
                    if (ar.succeeded()) {
                        LOGGER.info("HTTP server running on port 80");
                        future.complete();
                    } else {
                        LOGGER.error("Could not start a HTTP server", ar.cause());
                        future.fail(ar.cause());
                    }
                });
        return future;
    }

    private void indexHandler(RoutingContext context) {
        context.response().end("Hello from Vert.x!");
    }

    private void qrcodeHandler(RoutingContext context) {
        String type = context.request().getParam("type");
        String message = context.request().getParam("data");

        String requestKey = String.format("q1/type=%s&data=%s", type, message);
        // look for it in key value store
        String resp = cache.get(requestKey);

        if (resp == null) {
            resp = executeQRCodeRequest(type, message);
            cache.put(requestKey, resp);
        }
        context.response().end(resp);
    }

//    private String executeQRCodeRequest(String type, String message) {
//        String result = "";
//        if (type.equals("encode")) {
//            result = parser.encode(message, true);
//        } else if (type.equals("decode")) {
//            try {
//                result = parser.decode(message);
//            } catch (QRCodeParser.QRParsingException e) {
//                result = "decoding error";
//            }
//        }
//        return result;
//    }

}
