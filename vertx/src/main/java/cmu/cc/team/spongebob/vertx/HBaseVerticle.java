package cmu.cc.team.spongebob.vertx;

import cmu.cc.team.spongebob.query1.qrcode.QRCodeParser;
import cmu.cc.team.spongebob.query2.database.ContactUser;
import cmu.cc.team.spongebob.query2.database.TweetIntimacyHBaseBackend;
import cmu.cc.team.spongebob.query3.database.TopicWordHBaseBackend;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HBaseVerticle extends AbstractVerticle {
    /**
     * Response header.
     */
    private final String TEAMID = System.getenv("TEAM_ID");
    private final String TEAM_AWS_ACCOUNT_ID = System.getenv("TEAM_AWS_ID");
    private final String header = String.format("%s,%s\n", TEAMID, TEAM_AWS_ACCOUNT_ID);
    /**
     * Query 1.
     */
    private QRCodeParser qrCodeParser;
    /**
     * Query 2.
     */
    private TweetIntimacyHBaseBackend tweetIntimacyDBReader;
    /**
     * Query 3.
     */
    private static TopicWordHBaseBackend topicWordDBReader;

    private static WorkerExecutor executor;

    private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

    public HBaseVerticle() throws IOException {
        qrCodeParser = new QRCodeParser();
        tweetIntimacyDBReader = new TweetIntimacyHBaseBackend();
        topicWordDBReader = new TopicWordHBaseBackend();
    }

    @Override
    public void start(Future<Void> startFuture) {
        executor = vertx.createSharedWorkerExecutor("query2-worker-pool", 50);
        Future<Void> steps = startHttpServer();
        startFuture.complete();
    }

    private Future<Void> startHttpServer() {
        Future<Void> future = Future.future();
        HttpServer server = vertx.createHttpServer();

        Router router = Router.router(vertx);
        router.get("/").handler(this::indexHandler);
        router.get("/q1").handler(this::qrcodeHandler);
        router.get("/q2").handler(this::tweetIntimacyHBaseHandler);
        router.get("/q3").handler(this::topicWordHBaseHandler);

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

    private String executeQRCodeRequest(String type, String message) {
        String result = "";
        if (type.equals("encode")) {
            result = qrCodeParser.encode(message, true);
        } else if (type.equals("decode")) {
            try {
                result = qrCodeParser.decode(message);
            } catch (QRCodeParser.QRParsingException e) {
                result = "decoding error";
            }
        }
        return result;
    }

    private void qrcodeHandler(RoutingContext context) {
        String type = context.request().getParam("type");
        String message = context.request().getParam("data");

        String requestKey = String.format("q1/type=%s&data=%s", type, message);
        String resp = executeQRCodeRequest(type, message);

        context.response().end(resp);
    }

    private String queryHBase(Long userId, String phrase, int n) {
        ArrayList<ContactUser> contactUsers = tweetIntimacyDBReader.query(userId, phrase);
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

    private void tweetIntimacyHBaseHandler(RoutingContext context) {
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
        executor.<String>executeBlocking(future -> {
            String queryRes = queryHBase(userId, phrase, n);
            try {
                context.response().end(resp + queryRes);
            } catch (IllegalStateException e) {
                System.out.println("Response closed");
            }
            future.complete(queryRes);
        }, false, res-> {
            if (res.succeeded()) {
            } else {
                res.cause().printStackTrace();
            }
        });
    }

    private void topicWordHBaseHandler(RoutingContext context) {
        final String uidStartStr = context.request().getParam("uid_start");
        final String uidEndStr = context.request().getParam("uid_end");
        final String timeStartStr = context.request().getParam("time_start");
        final String timeEndStr = context.request().getParam("time_end");
        final String n1Str = context.request().getParam("n1");
        final String n2Str = context.request().getParam("n2");
        if (uidStartStr == null || uidEndStr == null
                || timeStartStr == null || timeEndStr == null
                || n1Str == null || n2Str == null) {
            context.response().end(header);
            return;
        }
        final Long uidStart = Long.parseLong(uidStartStr);
        final Long uidEnd = Long.parseLong(uidEndStr);
        final Long timeStart = Long.parseLong(timeStartStr);
        final Long timeEnd = Long.parseLong(timeEndStr);
        final int n1 = Integer.parseInt(n1Str);
        final int n2 = Integer.parseInt(n2Str);

        WorkerExecutor executor;
        executor = vertx.createSharedWorkerExecutor("query3-worker-pool", 50);
        executor.<String>executeBlocking(future -> {
            String queryRes = topicWordDBReader.query(uidStart, uidEnd, timeStart, timeEnd, n1, n2);
            try {
                context.response().end(header + queryRes);
            } catch (IllegalStateException e) {
                System.out.println("Response closed");
            }
            future.complete(queryRes);
        }, false, res-> {
            executor.close();
        });
    }
}