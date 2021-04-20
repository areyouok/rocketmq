package org.apache.rocketmq.client.impl.consumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author huangli
 * Created on 2021-04-21
 */
public class LogUtil {
    public static void log(String str, Object... args) {
        System.out.printf(new SimpleDateFormat("HH:mm:ss,SSS ").format(new Date()) + str + "\n", args);
    }
}
