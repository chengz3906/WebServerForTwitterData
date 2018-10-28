package cmu.cc.team.spongebob.query2.servlet;

import cmu.cc.team.spongebob.query2.database.ContactUser;
import cmu.cc.team.spongebob.query2.database.TweetIntimacyMySQLBackend;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;


public class TwitterIntimacyServlet extends HttpServlet{
    private TweetIntimacyMySQLBackend dbReader;
    private final String TEAMID = System.getenv("TEAMID");
    private final String TEAM_AWS_ACCOUNT_ID = System.getenv("TEAM_AWS_ACCOUNT_ID");

    public void init() {
        dbReader = new TweetIntimacyMySQLBackend();
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

        ArrayList<ContactUser> contactUsers = dbReader.query(userId, phrase);
        n = n > contactUsers.size() ? contactUsers.size() : n;
        for (int i = 0; i < n; ++i) {
            ContactUser contactUser = contactUsers.get(i);
            out.print(String.format("%s\t%s\t%s",
                    contactUser.getUserName(),
                    contactUser.getUserDescription(),
                    contactUser.getTweetText()));

            // output new line if it is not the last line
            if (i < n - 1) {
                out.print("\n");
            }
        }
    }
}
