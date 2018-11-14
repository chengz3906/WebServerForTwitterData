package cmu.cc.team.spongebob.vertx;

import cmu.cc.team.spongebob.qrcode.QRCodeParser;
import cmu.cc.team.spongebob.query2.database.ContactUser;
import cmu.cc.team.spongebob.query2.database.TweetIntimacyMySQLBackend;
import cmu.cc.team.spongebob.query3.database.MySQLResultSetWrapper;
import cmu.cc.team.spongebob.query3.database.TopicScoreCalculator;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MySQLVerticle extends AbstractVerticle {
    /**
     * Response header
     */
    private final String TEAMID = System.getenv("TEAMID");
    private final String TEAM_AWS_ACCOUNT_ID = System.getenv("TEAM_AWS_ACCOUNT_ID");
    private final String header = String.format("%s,%s\n", TEAMID, TEAM_AWS_ACCOUNT_ID);
    /**
     * MySQL Database name.
     */
    private static final String DB_NAME = System.getenv("MYSQL_DB_NAME");
    /**
     * DNS of Mysql database
     */
    private static final String DNS = System.getenv("MYSQL_DNS");
    /**
     * MySQL Username and password.
     */
    private static final String DB_USER = System.getenv("MYSQL_USER");
    private static final String DB_PWD = System.getenv("MYSQL_PWD");

    private final int MAX_POOL_SIZE = 500;
    // Vert.x MySQL client
    private SQLClient mySQLClient;

    private QRCodeParser qrCodeParser;

    private static WorkerExecutor executor;
    private TweetIntimacyMySQLBackend dbReader;
//    private TweetIntimacyHBaseBackend dbReader;

    private static TopicScoreCalculator topicScoreCalculator;
//    private static TopicWordHBaseBackend topicWordDBReader;

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLVerticle.class);

    public MySQLVerticle() throws IOException {
        qrCodeParser = new QRCodeParser();
        dbReader = new TweetIntimacyMySQLBackend();
//        dbReader = new TweetIntimacyHBaseBackend();
//        topicWordDBReader = new TopicWordHBaseBackend();
        topicScoreCalculator = new TopicScoreCalculator();
    }

    @Override
    public void start(Future<Void> startFuture) {
//        executor = vertx.createSharedWorkerExecutor("query2-worker-pool", 50);
//        Future<Void> steps = startHttpServer();
//        startFuture.complete();
        Future<Void> steps = prepareDatabase().compose(v -> startHttpServer());
        steps.setHandler(startFuture.completer());
    }

    private Future<Void> prepareDatabase() {
        Future<Void> future = Future.future();
        JsonObject mySQLClientConfig = new JsonObject()
                .put("host", DNS)
                .put("username", DB_USER)
                .put("password", DB_PWD)
                .put("database", DB_NAME)
                .put("maxPoolSize", MAX_POOL_SIZE);

        mySQLClient = MySQLClient.createNonShared(vertx, mySQLClientConfig);

        future.complete();

        return future;
    }

    private Future<Void> startHttpServer() {
        Future<Void> future = Future.future();
        HttpServer server = vertx.createHttpServer();

        Router router = Router.router(vertx);
        router.get("/").handler(this::indexHandler);
        router.get("/q1").handler(this::qrcodeHandler);
        router.get("/q2").handler(this::tweetIntimacyMySQLHandler);
        router.get("/q3").handler(this::topicWordMySQLHandler);

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

        String result = "";

        if (type != null && message != null) {
            if (type.equals("encode") && message.length() <= 22) {
                result = qrCodeParser.encode(message, true);
            } else if (type.equals("decode")) {
                try {
                    result = qrCodeParser.decode(message);
                } catch (QRCodeParser.QRParsingException e) {
                    result = "decoding error";
                }
            }
        }

        context.response().end(result);
    }

    private void tweetIntimacyMySQLHandler(RoutingContext context) {
        final String userIdStr = context.request().getParam("user_id");
        final String phrase = context.request().getParam("phrase");
        final String nStr = context.request().getParam("n");
        if (userIdStr == null || userIdStr.isEmpty()
                || phrase == null || phrase.isEmpty()
                || nStr == null || nStr.isEmpty()) {
            context.response().end(header);
            return;
        }
        mySQLClient.getConnection(car -> {
            if (car.succeeded()) {
                SQLConnection connection = car.result();

                final String sql = "SELECT user2_id, tweet_text, intimacy_score, "
                        + "user2_screen_name, user2_desc, created_at FROM "
                        + "contact_tweet WHERE user1_id=? "
                        + "ORDER BY user2_id ASC, created_at DESC";
                final Long userId = Long.parseLong(userIdStr);
                final int n = Integer.parseInt(nStr);

                final JsonArray params = new JsonArray().add(userId);

                connection.queryWithParams(sql, params, res -> {
                    connection.close();
                    if (res.succeeded()) {
                        ArrayList<ContactUser> contacts = new ArrayList<>();
                        Long lastUid = null;
                        ResultSet resultSet = res.result();
                        List<JsonObject> rows = resultSet.getRows();

                        for (JsonObject row: rows) {
                            Long uid = row.getLong("user2_id");
                            String text = row.getString("tweet_text");
                            double intimacyScore = row.getDouble("intimacy_score");
                            String screenName = row.getString("user2_screen_name");
                            String desc = row.getString("user2_desc");
                            String createdAt = row.getString("created_at");
                            if (!uid.equals(lastUid)) {
                                contacts.add(new ContactUser(uid, screenName,
                                        desc, intimacyScore));
                                lastUid = uid;
                            }
                            contacts.get(contacts.size() - 1).addTweet(text, phrase, createdAt);
                        }

                        Collections.sort(contacts);
                        String resp = "";
                        int numTweets = n > contacts.size() ? contacts.size() : n;
                        for (int i = 0; i < numTweets; ++i) {
                            ContactUser contactUser = contacts.get(i);
                            resp += String.format("%s\t%s\t%s",
                                    contactUser.getUserName(),
                                    contactUser.getUserDescription(),
                                    contactUser.getTweetText());

                            // output new line if it is not the last line
                            if (i < numTweets - 1) {
                                resp += "\n";
                            }
                        }
//
                        context.response().end(header + resp);
                    } else {
                        LOGGER.error("Could not get query", res.cause());
                        context.fail(res.cause());
                    }
                });
            } else {
                LOGGER.error("Could not connect to MySQL DB", car.cause());
                context.fail(car.cause());
            }
        });
    }

    private void topicWordMySQLHandler(RoutingContext context) {
        final String uidStartStr = context.request().getParam("uid_start");
        final String uidEndStr = context.request().getParam("uid_end");
        final String timeStartStr = context.request().getParam("time_start");
        final String timeEndStr = context.request().getParam("time_end");
        final String n1Str = context.request().getParam("n1");
        final String n2Str = context.request().getParam("n2");
        if (uidStartStr == null || uidStartStr.isEmpty()
                || uidEndStr == null || uidEndStr.isEmpty()
                || timeStartStr == null || timeStartStr.isEmpty()
                || timeEndStr == null || timeEndStr.isEmpty()
                || n1Str == null || n1Str.isEmpty()
                || n2Str == null || n2Str.isEmpty()) {
            context.response().end(header);
            return;
        }
        mySQLClient.getConnection(car -> {
            if (car.succeeded()) {
                SQLConnection connection = car.result();

                final String sql = "SELECT tweet_id, text, censored_text, impact_score "
                        + "FROM topic_word "
                        + "WHERE (user_id BETWEEN ? AND ?) "
                        + "AND (created_at BETWEEN ? AND ?) ";
                final Long uidStart = Long.parseLong(uidStartStr);
                final Long uidEnd = Long.parseLong(uidEndStr);
                final Long timeStart = Long.parseLong(timeStartStr);
                final Long timeEnd = Long.parseLong(timeEndStr);
                final int n1 = Integer.parseInt(n1Str);
                final int n2 = Integer.parseInt(n2Str);

                final JsonArray params = new JsonArray()
                        .add(uidStart).add(uidEnd)
                        .add(timeStart).add(timeEnd);

                connection.queryWithParams(sql, params, res -> {
                    connection.close();
                    if (res.succeeded()) {
                        ResultSet resultSet = res.result();
                        MySQLResultSetWrapper rsWrapper = new MySQLResultSetWrapper(resultSet);
                        String resp = topicScoreCalculator.getTopicScore(rsWrapper, n1, n2);
                        context.response().end(header + resp);
                    } else {
                        LOGGER.error("Could not get query", res.cause());
                        context.fail(res.cause());
                    }
                });
            } else {
                LOGGER.error("Could not connect to MySQL DB", car.cause());
                context.fail(car.cause());
            }
        });
    }

}

