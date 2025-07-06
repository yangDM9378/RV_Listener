import java.io.*;
import java.net.*;
import java.util.Set;
import java.util.concurrent.*;

public class SocketListenerV3 {
    private int socketPort;
    private Set<String> tagKeys;
    private Set<String> fieldKeys;
    private String timeKey;
    private BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();

    public SocketListenerV3(int socketPort, Set<String> tagKeys, Set<String> fieldKeys, String timeKey) {
        this.socketPort = socketPort;
        this.tagKeys = tagKeys;
        this.fieldKeys = fieldKeys;
        this.timeKey = timeKey;
    }

    public void start() {
        // 소켓 리스닝 쓰레드
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(socketPort)) {
                System.out.println("SocketListener started on port " + socketPort);
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), "UTF-8"));
                    StringBuilder xmlBuilder = new StringBuilder();
                    String line;
                    while ((line = in.readLine()) != null) {
                        xmlBuilder.append(line);
                        // 메시지 경계 검증 (예: </message>로 종료)
                        if (line.contains("</message>")) {
                            messageQueue.put(xmlBuilder.toString());
                            xmlBuilder.setLength(0);
                        }
                    }
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
                    XmlParsingV3.parseXmlData(xmlData, tagKeys, fieldKeys, timeKey);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }).start();
    }
}
