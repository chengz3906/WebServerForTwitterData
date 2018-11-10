package cmu.cc.team.spongebob.vertx;

import cmu.cc.team.spongebob.query1.qrcode.QRCodeParser;
import cmu.cc.team.spongebob.query2.database.ContactUser;
import cmu.cc.team.spongebob.query2.database.TweetIntimacyHBaseBackend;
import cmu.cc.team.spongebob.query2.database.TweetIntimacyMySQLBackend;
import cmu.cc.team.spongebob.utils.caching.KeyValueLRUCache;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpServer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.ArrayList;

public class MainVerticle extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);
    private final String TEAMID = System.getenv("TEAMID");
    private final String TEAM_AWS_ACCOUNT_ID = System.getenv("TEAM_AWS_ACCOUNT_ID");
    private TweetIntimacyMySQLBackend dbReader;
//    private TweetIntimacyHBaseBackend dbReader;

    private QRCodeParser parser;
    private KeyValueLRUCache cache;

    public MainVerticle () {
        parser = new QRCodeParser();
        cache = KeyValueLRUCache.getInstance();
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        parser = new QRCodeParser();
        dbReader = new TweetIntimacyMySQLBackend();
//        dbReader = new TweetIntimacyHBaseBackend();
        cache = KeyValueLRUCache.getInstance();
        Future<Void> steps = startHttpServer();
        startFuture.complete();
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
        String resp = cache.get(requestKey);

        if (resp == null) {
            resp = executeQRCodeRequest(type, message);
            cache.put(requestKey, resp);
        }
        context.response().end(resp);
    }

    private String queryDatabase(Long userId, String phrase, int n) {
        ArrayList<ContactUser> contactUsers = dbReader.query(userId, phrase);
        n = n > contactUsers.size() ? contactUsers.size() : n;
        String info = "";
        for (int i = 0; i < n; ++i) {
            ContactUser contactUser = contactUsers.get(i);
            info += String.format("%s\t%s\t%s",
                    contactUser.getUserName(),
                    contactUser.getUserDescription(),
                    contactUser.getTweetText());

            // output new line if it is not the last line
            if (i < n - 1) {
                info += "\n";
            }
        }
        return info;
    }
    private void tweetIntimacyHandler(RoutingContext context) {
        String resp = String.format("%s,%s\n", TEAMID, TEAM_AWS_ACCOUNT_ID);
        String phrase = context.request().getParam("phrase");
        String userIdStr = context.request().getParam("user_id");
        String nStr = context.request().getParam("n");
        if (phrase == null || phrase.isEmpty()
                || userIdStr == null || userIdStr.isEmpty()
                || nStr == null || nStr.isEmpty()) {
            context.response().end(resp);
            return;
        }
        Long userId = Long.parseLong(userIdStr);
        int n = Integer.parseInt(nStr);

        // Query cache
        String requestKey = String.format("q2/user_id=%s&phrase=%s&n=%s",
                userIdStr, phrase, nStr);
        String info = cache.get(requestKey);
        if (info != null) {
            context.response().end(resp + info);
            return;
        }

        WorkerExecutor executor;
        executor = vertx.createSharedWorkerExecutor("query2-worker-pool", 50);
        executor.<String>executeBlocking(future -> {
            String queryRes = queryDatabase(userId, phrase, n);
            try {
                context.response().end(resp + queryRes);
            } catch (IllegalStateException e) {
                System.out.println("Response closed");
            }
            future.complete(queryRes);
        }, false, res-> {
            if (res.succeeded()) {
                cache.put(requestKey, res.result());
            } else {
                res.cause().printStackTrace();
            }
            executor.close();
        });
    }

    private void mysqlHandler(RoutingContext context) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                String resp = String.format("%s,%s\n", TEAMID, TEAM_AWS_ACCOUNT_ID);
                String phrase = context.request().getParam("phrase");
                String userIdStr = context.request().getParam("user_id");
                String nStr = context.request().getParam("n");
                if (phrase.isEmpty() || userIdStr.isEmpty()
                        || nStr.isEmpty()) {
                    context.response().end(resp);
                    return;
                }
                Long userId = Long.parseLong(userIdStr);
                int n = Integer.parseInt(nStr);

                // Query cache
                String requestKey = String.format("q2/user_id=%s&phrase=%s&n=%s",
                        userIdStr, phrase, nStr);
                String info = cache.get(requestKey);
                if (info != null) {
                    context.response().end(resp + info);
                    return;
                }

                // Query database
                ArrayList<ContactUser> contactUsers = dbReader.query(userId, phrase);
                n = n > contactUsers.size() ? contactUsers.size() : n;
                info = "";
                for (int i = 0; i < n; ++i) {
                    ContactUser contactUser = contactUsers.get(i);
                    info += String.format("%s\t%s\t%s",
                            contactUser.getUserName(),
                            contactUser.getUserDescription(),
                            contactUser.getTweetText());

                    // output new line if it is not the last line
                    if (i < n - 1) {
                        info += "\n";
                    }
                }
                resp += info;
                context.response().end(resp);
                cache.put(requestKey, info);
            }
        });
        thread.start();
    }

    private String executeQRCodeRequest(String type, String message) {
        String result = "";
        if (type.equals("encode")) {
            result = parser.encode(message, true);
        } else if (type.equals("decode")) {
            try {
                result = parser.decode(message);
            } catch (QRCodeParser.QRParsingException e) {
                result = "decoding error";
            }
        }
        return result;
    }

}
