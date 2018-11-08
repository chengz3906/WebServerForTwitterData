package cmu.cc.team.spongebob.vertx;

import cmu.cc.team.spongebob.qrcode.QRCodeParser;
import cmu.cc.team.spongebob.utils.caching.KeyValueLRUCache;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpServer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class MainVerticle extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

    private final QRCodeParser qrCodeParser;
    private final KeyValueLRUCache keyValueCache;

    public MainVerticle () {
        qrCodeParser = new QRCodeParser();
        keyValueCache = KeyValueLRUCache.getInstance();
        // TODO experiment with different pool size
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Future<Void> steps = startHttpServer();
        steps.setHandler(startFuture.completer());
    }

    private Future<Void> startHttpServer() {
        Future<Void> future = Future.future();
        HttpServer server = vertx.createHttpServer();

        Router router = Router.router(vertx);
        router.get("/").handler(this::indexHandler);
        router.get("/q1").handler(this::qrcodeHandler);
        router.get("/q2").handler(this::tweetIntimacyHandler);

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
        context.response().end("Heartbeat: Hello from Vert.x!");
    }

    private void qrcodeHandler(RoutingContext context) {
        String type = context.request().getParam("type");
        String message = context.request().getParam("data");

        String requestKey = String.format("q1/type=%s&data=%s", type, message);
        // look for it in key value store
        String resp = keyValueCache.get(requestKey);

        if (resp == null) {
            if (type.equals("encode")) {
                resp = qrCodeParser.encode(message, true);
            } else if (type.equals("decode")) {
                try {
                    resp = qrCodeParser.decode(message);
                } catch (QRCodeParser.QRParsingException e) {
                    resp = "decoding error";
                }
            }
            keyValueCache.put(requestKey, resp);
        }
        context.response().end(resp);
    }

    private void tweetIntimacyHandler(RoutingContext context) {
        vertx.executeBlocking(future -> {
            // TODO call tweet intimacy backend handler
            future.complete("TODO: add MySQL handler.");
        }, false, res -> {
            context.response().end((String) res.result());
        });
    }

    private void topicWordHandler(RoutingContext context) {
        // TODO query 3
    }
}
