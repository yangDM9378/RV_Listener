import java.io.*;
import java.net.*;
import java.util.Set;
import java.util.concurrent.*;

public class SocketListenerV3 {
    private int socketPort;
    private Set<String> tagKeys;
    private Set<String> fieldKeys;
    private String timeKey;
    private String influxV3Database;
    private String influxV3measurement;
    private BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();

    public SocketListenerV3(int socketPort, Set<String> tagKeys, Set<String> fieldKeys, String timeKey, String influxV3Database, String influxV3measurement) {
        this.socketPort = socketPort;
        this.tagKeys = tagKeys;
        this.fieldKeys = fieldKeys;
        this.timeKey = timeKey;
        this.influxV3Database = influxV3Database;
        this.influxV3measurement = influxV3measurement;
    }

    public void start() {
        // 소켓 리스닝 쓰레드
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(socketPort)) {
                System.out.println("SocketListener started on port " + socketPort);
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("[INFO] Client connected: " + clientSocket.getInetAddress());

                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), "UTF-8"));
                    StringBuilder xmlBuilder = new StringBuilder();
                    char[] buffer = new char[4096];
                    int len;
                    while ((len = in.read(buffer)) != -1) {
                        String chunk = new String(buffer, 0, len);
                        xmlBuilder.append(chunk);

                        // 메시지 끝 검출
                        if (xmlBuilder.toString().contains("</message>")) {
                            String completeXml = xmlBuilder.toString();
                            messageQueue.put(completeXml);
                            System.out.println("[INFO] messageQueue put xml data.");
                            xmlBuilder.setLength(0);
                        }
                    }

                    System.out.println("[INFO] Client disconnected.");
                    clientSocket.close();
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        // 파싱/적재 쓰레드
        new Thread(() -> {
            while (true) {
                try {
                    String xmlData = messageQueue.take();
                    System.out.println("[INFO] message parsing start");
                    LogUtils.writeLogToSubFolder("socket", "parsing_start", "start");
                    XmlParsingV3.parseXmlData(xmlData, tagKeys, fieldKeys, timeKey, influxV3Database, influxV3measurement);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }).start();
    }
}
