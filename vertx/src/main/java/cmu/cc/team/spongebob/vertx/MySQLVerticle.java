package cmu.cc.team.spongebob.vertx;

import cmu.cc.team.spongebob.qrcode.QRCodeParser;
import cmu.cc.team.spongebob.query2.ContactUser;
import cmu.cc.team.spongebob.query3.TopicScoreCalculator;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLRowStream;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

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
     * MySQL Client config
     */
    private static final String DB_USER = System.getenv("MYSQL_USER");
    private static final String DB_PWD = System.getenv("MYSQL_PWD");
    private final int MAX_POOL_SIZE = 500;

    private SQLClient mySQLClient;

    /*
     * Backend logic
     */
    private QRCodeParser qrCodeParser;
    private static TopicScoreCalculator topicScoreCalculator;

    /*
     * sql queries
     */
    private final String query2SQL;
    private final String query3SQL;

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLVerticle.class);

    public MySQLVerticle() throws IOException {
        qrCodeParser = new QRCodeParser();
        topicScoreCalculator = new TopicScoreCalculator();

        // load query 2 SQL statement
        ClassLoader classLoader = this.getClass().getClassLoader();
        StringBuilder q2StringBuilder = new StringBuilder();
        InputStream in = classLoader.getResourceAsStream("query2.sql");
        try (Scanner scanner = new Scanner(in)) {
            while (scanner.hasNextLine()) {
                q2StringBuilder.append(scanner.nextLine());
            }
        }
        query2SQL = q2StringBuilder.toString();
        LOGGER.info(query2SQL);

        // load query 3
        StringBuilder q3StringBuilder = new StringBuilder();
        in = classLoader.getResourceAsStream("query3.sql");
        try (Scanner scanner = new Scanner(in)) {
            while (scanner.hasNextLine()) {
                q3StringBuilder.append(scanner.nextLine());
            }
        }
        query3SQL = q3StringBuilder.toString();
        LOGGER.info(query3SQL);
    }

    @Override
    public void start(Future<Void> startFuture) {
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
        router.get("/q1").handler(this::qrCodeHandler);
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

    private void qrCodeHandler(RoutingContext context) {
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

                final Long userId = Long.parseLong(userIdStr);
                final int n = Integer.parseInt(nStr);

                final JsonArray params = new JsonArray().add(userId).add(userId);

                connection.queryStreamWithParams(query2SQL, params, res -> {
                    connection.close();
                    if (res.succeeded()) {
                        ArrayList<ContactUser> contacts = new ArrayList<>();
                        SQLRowStream sqlRowStream = res.result();
                        sqlRowStream
                                .resultSetClosedHandler(v -> {
                                    sqlRowStream.moreResults();
                                })
                                .handler(row -> {
                                    Long uid = row.getLong(0);
                                    String text = row.getString(1);
                                    double intimacyScore = row.getDouble(2);
                                    String screenName = row.getString(3);
                                    String desc = row.getString(4);
                                    Long createdAt = row.getLong(5);
                                    if (contacts.isEmpty() ||
                                            !uid.equals(contacts.get(contacts.size() - 1).getUserId())) {
                                        contacts.add(new ContactUser(uid, screenName,
                                                desc, intimacyScore));
                                    }
                                    contacts.get(contacts.size() - 1).addTweet(text, phrase, createdAt);
                                })
                                .endHandler(v -> {
                                    // Sort contacts
                                    PriorityQueue<ContactUser> sortedContacts = new PriorityQueue<>();
                                    for (ContactUser cu : contacts) {
                                        sortedContacts.add(cu);
                                        if (sortedContacts.size() > n) {
                                            sortedContacts.poll();
                                        }
                                    }
                                    ArrayList<ContactUser> reversedContacts = new ArrayList<>();
                                    while (!sortedContacts.isEmpty()) {
                                        reversedContacts.add(0, sortedContacts.poll());
                                    }
                                    StringBuilder respStringBuilder = new StringBuilder();
                                    for (ContactUser contactUser : reversedContacts) {
                                        respStringBuilder.append(String.format("%s\t%s\t%s\n",
                                                contactUser.getUserName(),
                                                contactUser.getUserDescription(),
                                                contactUser.getTweetText()));
                                    }
                                    respStringBuilder.deleteCharAt(respStringBuilder.length() - 1);
                                    context.response().end(header + respStringBuilder);
                                });
                    } else {
                        LOGGER.error("Could not get query", res.cause());
                        context.fail(res.cause());
                    }
                });
                /*
                connection.queryWithParams(query2SQL, params, res -> {
                    connection.close();
                    if (res.succeeded()) {
                        ArrayList<ContactUser> contacts = new ArrayList<>();
                        ResultSet rs = res.result();
                        List<JsonObject> rows = rs.getRows();
                        for (JsonObject row : rows) {
                            Long uid = row.getLong("user_id");
                            String text = row.getString("tweet_text");
                            double intimacyScore = row.getDouble("intimacy_score");
                            String screenName = row.getString("user_screen_name");
                            String desc = row.getString("user_desc");
                            Long createdAt = row.getLong("created_at");
                            if (contacts.isEmpty() ||
                                    !uid.equals(contacts.get(contacts.size() - 1).getUserId())) {
                                contacts.add(new ContactUser(uid, screenName,
                                        desc, intimacyScore));
                            }
                            contacts.get(contacts.size() - 1).addTweet(text, phrase, createdAt);
                        }
                        // Sort contacts
                        PriorityQueue<ContactUser> sortedContacts = new PriorityQueue<>();
                        for (ContactUser cu : contacts) {
                            sortedContacts.add(cu);
                            if (sortedContacts.size() > n) {
                                sortedContacts.poll();
                            }
                        }
                        ArrayList<ContactUser> reversedContacts = new ArrayList<>();
                        while (!sortedContacts.isEmpty()) {
                            reversedContacts.add(0, sortedContacts.poll());
                        }
                        StringBuilder respStringBuilder = new StringBuilder();
                        for (ContactUser contactUser : reversedContacts) {
                            respStringBuilder.append(String.format("%s\t%s\t%s\n",
                                    contactUser.getUserName(),
                                    contactUser.getUserDescription(),
                                    contactUser.getTweetText()));
                        }
                        respStringBuilder.deleteCharAt(respStringBuilder.length() - 1);
                        context.response().end(header + respStringBuilder);
                    } else {
                        LOGGER.error("Could not get query", res.cause());
                        context.fail(res.cause());
                    }
                });
                */
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

                final Long uidStart = Long.parseLong(uidStartStr);
                final Long uidEnd = Long.parseLong(uidEndStr);
                final Long timeStart = Long.parseLong(timeStartStr);
                final Long timeEnd = Long.parseLong(timeEndStr);
                final int n1 = Integer.parseInt(n1Str);
                final int n2 = Integer.parseInt(n2Str);

                final JsonArray params = new JsonArray()
                        .add(uidStart).add(uidEnd)
                        .add(timeStart).add(timeEnd);

                connection.queryStreamWithParams(query3SQL, params, res -> {
                    connection.close();
                    if (res.succeeded()) {
                        SQLRowStream sqlRowStream = res.result();
                        topicScoreCalculator.getTopicScore(sqlRowStream, n1, n2, context, header);
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