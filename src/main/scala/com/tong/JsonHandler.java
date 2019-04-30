//package com.tong;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.google.gson.JsonSyntaxException;
//import org.apache.flume.Context;
//import org.apache.flume.Event;
//import org.apache.flume.FlumeException;
//import org.apache.flume.event.EventBuilder;
//import org.apache.flume.event.JSONEvent;
//import org.apache.flume.source.http.HTTPBadRequestException;
//import org.apache.flume.source.http.HTTPSourceHandler;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.servlet.http.HttpServletRequest;
//import java.awt.print.Printable;
//import java.io.BufferedReader;
//import java.io.UnsupportedEncodingException;
//import java.nio.charset.UnsupportedCharsetException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
///**
// * @author : JeffOsmond
// * @version : v1.0
// * @function : Flume自定义JsonHandler
// * @createTime : 2018/12/13 11:54
// */
//public class JsonHandler {
//
//    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flume.source.http.JSONHandler.class);
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public List<Event> getEvents(HttpServletRequest request) throws Exception {
//        BufferedReader reader = request.getReader();
//        String charset = request.getCharacterEncoding();
//        //UTF-8 is default for JSON. If no charset is specified, UTF-8 is to
//        //be assumed.
//        if (charset == null) {
//            LOG.debug("Charset is null, default charset of UTF-8 will be used.");
//            charset = "UTF-8";
//        } else if (!(charset.equalsIgnoreCase("utf-8")
//                || charset.equalsIgnoreCase("utf-16")
//                || charset.equalsIgnoreCase("utf-32"))) {
//            LOG.error("Unsupported character set in request {}. "
//                    + "JSON handler supports UTF-8, "
//                    + "UTF-16 and UTF-32 only.", charset);
//            throw new UnsupportedCharsetException("JSON handler supports UTF-8, "
//                    + "UTF-16 and UTF-32 only.");
//        }
//
//    /*
//     * Gson throws Exception if the data is not parseable to JSON.
//     * Need not catch it since the source will catch it and return error.
//     */
//        List eventList = new ArrayList();
//        //BufferReader转String
//        StringBuffer buffer = new StringBuffer();
//        String line = " ";
//        while ((line = reader.readLine()) != null) {
//            buffer.append(line);
//        }
//        String jsonStr = new String(buffer.toString().getBytes(), charset);
//        try {
//           List  lists = (List)JSON.parseObject(jsonStr.replace("\\\"", "!*!"), ArrayList.class);
//            for (Object list : lists) {
//                JSONEvent jsonEvent = new JSONEvent();
//                jsonEvent.setBody(((JSONObject)list).getString("body").toString().getBytes());
//                eventList.add(jsonEvent);
//            }
//        } catch (JsonSyntaxException ex) {
//            throw new HTTPBadRequestException("Request has invalid JSON Syntax.", ex);
//        }
//        return getSimpleEvents(eventList);
//    }
//
//    @Override
//    public void configure(Context context) {
//    }
//
//    private List<Event> getSimpleEvents(List<Event> events) {
//        List<Event> newEvents = new ArrayList<Event>(events.size());
//        for (Event e : events) {
//            newEvents.add(EventBuilder.withBody(e.getBody(), e.getHeaders()));
//            System.out.println(e.getBody().toString());
//        }
//        return newEvents;
//    }
//}