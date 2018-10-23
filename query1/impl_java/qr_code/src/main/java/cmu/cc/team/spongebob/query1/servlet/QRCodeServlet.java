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

        String requestKey = String.format("q1/type=%s&data=%s", type, message);

        // look for it in key value store
        String resp = cache.get(requestKey);
        PrintWriter out = response.getWriter();

        if (resp == null) {
            resp = executeQRCodeRequest(type, message);
            out.print(resp);

            cache.put(requestKey, resp);
        } else {
            out.print(resp);
        }
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
