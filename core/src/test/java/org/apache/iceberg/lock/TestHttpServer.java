package org.apache.iceberg.lock;


import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Map;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

public class TestHttpServer implements Runnable, Closeable {

    private final static int PORT = 28081;
    private ServerSocket server = null;
    private final static String RESP = "OK!";
    private final static String PATH_DEF = "/test-rest-lock?";
    private transient boolean running = true;
    private final Map<String, String> cache = new ConcurrentHashMap<>();

    public TestHttpServer() {
        try {
            server = new ServerSocket(PORT);
            if (server == null)
                System.exit(1);
            new Thread(this).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while (running) {
            try(Socket client = server.accept()){
                if(client!=null){
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(client.getInputStream()));
                    // GET /apple /HTTP1.1
                    String line = reader.readLine();
                    String resource = line.substring(line.indexOf('/'),
                            line.lastIndexOf('/') - 5);
                    resource = URLDecoder.decode(resource, "UTF-8");
                    String method = new StringTokenizer(line).nextElement()
                            .toString();
                    while ((line = reader.readLine()) != null) {
                        if (line.equals("")) {
                            break;
                        }
                    }
                    if ("GET".equalsIgnoreCase(method) && resource.startsWith(PATH_DEF)) {
                        URI uri = URI.create(resource);
                        String paramList = uri.getQuery();
                        String [] params = paramList.split("&");
                        String ownerId = null;
                        String entityId = null;
                        String operator = null;
                        for (String paramKV : params) {
                            String [] paramPair = paramKV.split("=");
                            String key = paramPair[0];
                            String value = paramPair[1];
                            if("ownerId".equals(key)){
                                ownerId = value;
                            }
                            if("entityId".equals(key)){
                                entityId = value;
                            }
                            if("operator".equals(key)){
                                operator = value;
                            }
                        }
                        if("lock".equals(operator)){
                            if(cache.containsKey(entityId)){
                                doResp(client,"HTTP/1.0 500 ERROR",null);
                                continue;
                            }
                            String realOwnerId = cache.putIfAbsent(entityId,ownerId);
                            if(!Objects.equals(ownerId,realOwnerId) && realOwnerId!=null){
                                doResp(client,"HTTP/1.0 500 ERROR",null);
                                continue;
                            }
                        }else if("unlock".equals(operator)){
                            cache.remove(entityId);
                        }else{
                            doResp(client,"HTTP/1.0 500 ERROR",null);
                            continue;
                        }
                        doResp(client,"HTTP/1.0 200 OK",RESP);
                    } else {
                        doResp(client,"HTTP/1.0 404 Not found",null);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void doResp(Socket client,String respLine,String body) throws IOException {
        PrintStream writer = new PrintStream(client.getOutputStream());
        writer.println(respLine);
        if(body!=null){
            writer.println("Content-Length:" + body.length());
        }
        writer.println();
        if(body!=null){
            writer.println(body);
        }
    }


    @Override
    public void close() throws IOException {
        running=false;
        if(server!=null && !server.isClosed()){
            server.close();
        }
    }

}