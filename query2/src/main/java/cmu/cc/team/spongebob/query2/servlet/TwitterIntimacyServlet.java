package cmu.cc.team.spongebob.query2.servlet;

import cmu.cc.team.spongebob.query2.database.DBReader;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;


public class TwitterIntimacyServlet extends HttpServlet{
    private DBReader dbReader;
    private final String TEAMID = "Spongebob";
    private final String TEAM_AWS_ACCOUNT_ID = "859423033203";

    public void init() {
        dbReader = new DBReader();
    }
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
//        response.setContentType("text/html");
//        PrintWriter writer = response.getWriter();
//        writer.println("<h1>Query2 Twitter Intimacy Servlet Test View<h1>");
        String phrase = request.getParameter("phrase");
        Long userId = Long.parseLong(request.getParameter("user_id"));
        int n = Integer.parseInt(request.getParameter("n"));

        ArrayList<String> userName = new ArrayList<>();
        ArrayList<String> userDesc = new ArrayList<>();
        ArrayList<String> contactTweet = new ArrayList<>();
        dbReader.query(userId, phrase, n, userName, userDesc, contactTweet);

        PrintWriter out = response.getWriter();
        out.print(TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n");
        for (int i = 0; i < userName.size(); ++i) {
            out.print(userName.get(i) + "\t"
                + userDesc.get(i) + "\t"
                + contactTweet.get(i) + "\n");
        }
    }
}
