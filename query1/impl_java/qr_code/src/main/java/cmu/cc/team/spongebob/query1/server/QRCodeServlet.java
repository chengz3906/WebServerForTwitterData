package cmu.cc.team.spongebob.query1.server;

import cmu.cc.team.spongebob.query1.qrcode.QRCodeParser;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class QRCodeServlet extends HttpServlet {
    private QRCodeParser parser;

    public void init() throws ServletException {
        parser = new QRCodeParser();
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String type = request.getParameter("type");
        String message = request.getParameter("data");

        String resp = "";
        if (type.equals("encode")) {
            resp = parser.encode(message, true);
        } else if (type.equals("decode")) {
            try {
                resp = parser.decode(message);
            } catch (QRCodeParser.QRParsingException e) {
                resp = "decoding error";
            }
        }

        PrintWriter out = response.getWriter();
        out.println(resp);
    }
}
