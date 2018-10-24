package cmu.cc.team.spongebob.query1.servlet;

import cmu.cc.team.spongebob.utils.caching.KeyValueLRUCache;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;


class QRCodeServletTest extends Mockito {
    @Test
    void testQRCodeEncode() throws Exception {
        QRCodeServlet  qrCodeServlet = new QRCodeServlet();
        qrCodeServlet.init();
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response =  mock(HttpServletResponse.class);
        when(request.getParameter("type")).thenReturn("encode");
        when(request.getParameter("data")).thenReturn("CC Team");

        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        when(response.getWriter()).thenReturn(writer);

        qrCodeServlet.doGet(request, response);

        assertEquals("0x66d92b800x5bc76d830x121a7fa60x51c111870x3a5f3ca30x8be36a130xedb223a0xfc8e98780x33bf50de0x2e8709700x545a2d0f0xecef7ae0x461175cd0xff132a",
                stringWriter.toString());
    }

    @Test
    void testQRCodeDecode() throws Exception {
        QRCodeServlet  qrCodeServlet = new QRCodeServlet();
        qrCodeServlet.init();
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response =  mock(HttpServletResponse.class);
        when(request.getParameter("type")).thenReturn("decode");
        when(request.getParameter("data")).thenReturn("0x2b23d6830x15a0de0d0x744784010x29e880700xfe1adf5c0xb96061290x1127b67c0x311690430xc63153140xf6e00650x92d3960b0xf59a79070x704e73d40x977fd8090xf516e98a0x3e0c19f10xac626d040x6a3e58650xca85aa3e0x6266b640x842ddcb40x4e7c879c0x85dd21240x3afae3dc0xe07908a70x664685970xb38246f70x511908330x40a111ee0xc12c8fd10x82984c520x4ddee6f6");

        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        when(response.getWriter()).thenReturn(writer);

        qrCodeServlet.doGet(request, response);

        assertEquals("CC Team is awesome!", stringWriter.toString());
    }

    @Test
    void testLoad() throws Exception {
        final long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            testQRCodeEncode();
            testQRCodeDecode();
        }
        final long endTime = System.currentTimeMillis();

        System.out.println("Total execution time: " + (endTime - startTime) + "ms");
    }

    @Test
    void testEncodeLoad() throws Exception {
        final long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            testQRCodeEncode();
            KeyValueLRUCache.getInstance().reset();
        }
        final long endTime = System.currentTimeMillis();

        System.out.println("Total execution time: " + (endTime - startTime) + "ms");
    }

    @Test
    void testDecodeLoad() throws Exception {
        final long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            testQRCodeDecode();
            KeyValueLRUCache.getInstance().reset();
        }
        final long endTime = System.currentTimeMillis();

        System.out.println("Total execution time: " + (endTime - startTime) + "ms");
    }
}
