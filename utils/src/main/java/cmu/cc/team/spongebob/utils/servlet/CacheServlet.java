package cmu.cc.team.spongebob.utils.servlet;

import cmu.cc.team.spongebob.utils.caching.KeyValueLRUCache;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class CacheServlet extends HttpServlet {

    private KeyValueLRUCache cache;

    public void init() {
        cache = KeyValueLRUCache.getInstance();
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws IOException {

        // Reset cache
        PrintWriter out = response.getWriter();
        String reset = request.getParameter("reset");
        String maxItemNumber = request.getParameter("max_item_number");
        if (!reset.isEmpty() && reset.equals("true")) {
            cache.reset();
            out.println("Cache reset complete!");
        }
        if (!maxItemNumber.isEmpty()) {
            cache.setMaxItemNumber(Long.parseLong(maxItemNumber));
            out.println("Max item number set to " + maxItemNumber);
        }

    }
}
