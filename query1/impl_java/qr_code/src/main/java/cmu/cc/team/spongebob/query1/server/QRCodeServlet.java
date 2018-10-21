package cmu.cc.team.spongebob.query1.server;

import cmu.cc.team.spongebob.query1.qrcode.QRCodeParser;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class QRCodeServlet extends HttpServlet {
    private QRCodeParser parser;
    private ConcurrentHashMap<String, String> encodeCache;
    private ConcurrentHashMap<String, String> decodeCache;

    public void init() throws ServletException {
        parser = new QRCodeParser();
        encodeCache = new ConcurrentHashMap<>();
        decodeCache = new ConcurrentHashMap<>();
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String type = request.getParameter("type");
        String message = request.getParameter("data");

        String resp = "";
        if (type.equals("encode")) {
            resp = encodeCache.getOrDefault(message, null);

            if (resp == null) {
                resp = parser.encode(message, true);
                encodeCache.put(message, resp);
            }
        } else if (type.equals("decode")) {
            resp = decodeCache.getOrDefault(message, null);

            if (resp == null) {
                try {
                    resp = parser.decode(message);
                } catch (QRCodeParser.QRParsingException e) {
                    resp = "decoding error";
                }
                decodeCache.put(message, resp);
            }
        }

        PrintWriter out = response.getWriter();
        out.print(resp);
    }

    public void destroy() {
        encodeCache.clear();
        decodeCache.clear();
    }
}
