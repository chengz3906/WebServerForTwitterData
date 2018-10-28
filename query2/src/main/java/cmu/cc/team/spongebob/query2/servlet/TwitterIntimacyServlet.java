package cmu.cc.team.spongebob.query2.servlet;

import cmu.cc.team.spongebob.query2.database.ContactUser;
import cmu.cc.team.spongebob.query2.database.TweetIntimacyMySQLBackend;
import cmu.cc.team.spongebob.utils.caching.KeyValueLRUCache;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class TwitterIntimacyServlet extends HttpServlet {
    private TweetIntimacyMySQLBackend dbReader;
    private KeyValueLRUCache cache;
    private final String TEAMID = System.getenv("TEAMID");
    private final String TEAM_AWS_ACCOUNT_ID = System.getenv("TEAM_AWS_ACCOUNT_ID");

    public void init() {
        dbReader = new TweetIntimacyMySQLBackend();
        cache = KeyValueLRUCache.getInstance();
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws IOException {

        response.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();
        out.print(String.format("%s,%s\n", TEAMID, TEAM_AWS_ACCOUNT_ID));

        String phrase = request.getParameter("phrase");
        String userIdStr = request.getParameter("user_id");
        String nStr = request.getParameter("n");
        if (phrase.isEmpty() || userIdStr.isEmpty()
            || nStr.isEmpty()) {
            return;
        }
        Long userId = Long.parseLong(userIdStr);
        int n = Integer.parseInt(nStr);

        // Query cache
        String requestKey = String.format("q2/user_id=%s&phrase=%s&n=%s",
                userIdStr, phrase, nStr);
        String resp = cache.get(requestKey);
        if (resp != null) {
            out.print(resp);
            return;
        }

        // Query database
        ArrayList<ContactUser> contactUsers = dbReader.query(userId, phrase);
        n = n > contactUsers.size() ? contactUsers.size() : n;
        resp = "";
        for (int i = 0; i < n; ++i) {
            ContactUser contactUser = contactUsers.get(i);
            resp += String.format("%s\t%s\t%s",
                    contactUser.getUserName(),
                    contactUser.getUserDescription(),
                    contactUser.getTweetText());

            // output new line if it is not the last line
            if (i < n - 1) {
                resp += "\n";
            }
        }
        out.print(resp);
        cache.put(requestKey, resp);
    }
}
