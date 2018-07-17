package dingbinthuAt163com.opensource.freedom.es.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author DingBin , dingbinthu@163.com
 * @date 08 18 2016, 13:23
 */
public class ExceptionUtil {

    public static String getTrace(Throwable throwable) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        throwable.printStackTrace(writer);
        StringBuffer buffer = stringWriter.getBuffer();
        return buffer.toString();
    }
}
