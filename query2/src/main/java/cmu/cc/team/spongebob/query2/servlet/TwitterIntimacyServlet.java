package cmu.cc.team.spongebob.query2.servlet;

import cmu.cc.team.spongebob.query2.database.DBReader;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;


public class TwitterIntimacyServlet extends HttpServlet{
    private DBReader dbReader;
    private final String TEAMID = System.getenv("TEAMID");
    private final String TEAM_AWS_ACCOUNT_ID = System.getenv("TEAM_AWS_ACCOUNT_ID");

    public void init() {
        dbReader = new DBReader();
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
        String phrase = request.getParameter("phrase");
        String userIdStr = request.getParameter("user_id");
        String nStr = request.getParameter("n");
        if (phrase.isEmpty() || userIdStr.isEmpty()
            || nStr.isEmpty()) {
            return;
        }
        Long userId = Long.parseLong(userIdStr);
        int n = Integer.parseInt(nStr);

        ArrayList<String> userName = new ArrayList<>();
        ArrayList<String> userDesc = new ArrayList<>();
        ArrayList<String> contactTweet = new ArrayList<>();
        dbReader.query(userId, phrase, n, userName, userDesc, contactTweet);

        response.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();
        out.print(String.format("%s,%s\n", TEAMID, TEAM_AWS_ACCOUNT_ID));
        for (int i = 0; i < userName.size(); ++i) {
            out.print(String.format("%s\t%s\t%s",
                    userName.get(i),
                    userDesc.get(i),
                    contactTweet.get(i)));

            // output new line if it is not the last line
            if (i < userName.size() - 1) {
                out.print("\n");
            }
        }
    }
}
