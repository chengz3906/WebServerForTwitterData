package cmu.cc.team.spongebob.query1.servlet;

import cmu.cc.team.spongebob.query1.qrcode.QRCodeParser;
import cmu.cc.team.spongebob.query1.utils.KeyValueLRUCache;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class QRCodeServlet extends HttpServlet {
    private static final int MAX_CACHE_SIZE = 200000;

    private QRCodeParser parser;
    private KeyValueLRUCache cache;

    public void init() throws ServletException {
        parser = new QRCodeParser();
        cache = KeyValueLRUCache.getInstance();
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String type = request.getParameter("type");
        String message = request.getParameter("data");

        String requestID = String.format("q1/type=%s&data=%s", type, message);

        // look for it in key value store
        String resp = cache.get(requestID);

        if (resp == null) {
            if (type.equals("encode")) {
                resp = parser.encode(message, true);
            } else if (type.equals("decode")) {
                try {
                    resp = parser.decode(message);
                } catch (QRCodeParser.QRParsingException e) {
                    resp = "decoding error";
                }
            }

            cache.put(requestID, resp);
        }

        PrintWriter out = response.getWriter();
        out.print(resp);
    }
}
