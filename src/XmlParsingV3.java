import org.w3c.dom.*;
import javax.xml.parsers.*;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;

public class XmlParsingV3 {
    public static void parseXmlData(String xmlData, Set<String> tagKeys, Set<String> fieldKeys, String timeKey, String influxV3Database, String influxV3measurement) {
        try {
            // string으로 가져온 xmlData를 트리형태 DOM 구조로 변경
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(xmlData.getBytes("UTF-8")));

            // messagename 추출하여 사용 검증
            NodeList messageNameList = doc.getElementsByTagName("messagename");
            if (messageNameList.getLength() == 0) {
                System.out.println("Error: no messagename tag, skip xml");
                return;
            }
            // messagname이 TraceData가 아닌 경우 제외 (대소문자 상관 없이)
            String messageName = messageNameList.item(0).getTextContent();
            if (!"TraceData".equalsIgnoreCase(messageName)) {
                System.out.println("Skip: messagename=" + messageName);
                return;
            }
            // MACHINENAME 추출 (DB 이름)
            String dbName = influxV3Database;
            NodeList dbNameList = doc.getElementsByTagName("MACHINENAME");
            if (dbNameList != null && dbNameList.getLength() > 0) {
                String extracted = dbNameList.item(0).getTextContent();
                if (extracted != null && !extracted.trim().isEmpty()) {
                    dbName = extracted;
                } else {
                    System.out.println("Warning: MACHINENAME value is empty, using fallback dbName = " + influxV3Database);
                }
            } else {
                System.out.println("Warning: no MACHINENAME tag found, using fallback dbName = " + influxV3Database);
            }
            Map<String, Object> tags = new HashMap<>();
            Map<String, Object> fields = new HashMap<>();
            String influxTimeString = null;
            String pendingHostCmdName = null;

            NodeList items = doc.getElementsByTagName("ITEM");
            for (int i = 0; i < items.getLength(); i++) {
                Element item = (Element) items.item(i);
                // ITEM안의 ITEMNAME
                NodeList itemNameList = item.getElementsByTagName("ITEMNAME");
                if (itemNameList.getLength() == 0) continue;
                // ITEM안의 ITEMNAME이 여러개인 경우 확인용 -> 첫번째만 저장되도록 구현됨 -> 제공된 log 형식 기준
                if (itemNameList.getLength() > 1) {
                    String firstItemName = itemNameList.item(0).getTextContent();
                    String logMessage = "Warning: Multiple ITEMNAME elements found in ITEM. Using the first one only: " + firstItemName;
                    // LogUtils.writeLogToSubFolder("xml_parsing", "parsing_warning", logMessage);
                }
                String itemName = itemNameList.item(0).getTextContent();

                // ITEMNAME이 SY_CLOCK과 같은 값을 time 값으로 설정하기
                if (timeKey.equals(itemName)) {
                    NodeList siteValueList = item.getElementsByTagName("SITEVALUE");
                    if (siteValueList.getLength() == 1) {
                        influxTimeString = siteValueList.item(0).getTextContent();
                    }
                    continue;
                }

                boolean isTagKey = tagKeys.contains(itemName);
                boolean isFieldKey = fieldKeys.contains(itemName);

                if (!isTagKey && !isFieldKey) continue;
                if (isTagKey && isFieldKey) {
                    String logMessage = "Skip: Config file contains duplicate key in both tag and field: " + itemName;
                    System.out.println(logMessage);
                    // LogUtils.writeLogToSubFolder("xml_parsing", "parsing_warning", logMessage);
                    continue;
                }

                NodeList siteValueList = item.getElementsByTagName("SITEVALUE");
                // SITEVALUE 태그 자체가 없는 경우
                if (siteValueList.getLength() == 0) {
                    String logMessage = "Warning: SITEVALUE tag not found. itemName=" + itemName;
                    System.out.println(logMessage);
                    // LogUtils.writeLogToSubFolder("xml_parsing", "parsing_warning", logMessage);
                    if (isTagKey) {
                        tags.put(itemName, "-");
                    } else if (isFieldKey) {
                        fields.put(itemName, "-");
                    }
                    continue;
                }

                String siteValue = siteValueList.item(0).getTextContent();
                // SITEVALUE 태그는 있으나 값이 비어 있는 경우
                if (siteValue == null || siteValue.trim().isEmpty()) {
                    String logMessage = "Warning: SITEVALUE is empty. itemName=" + itemName;
                    System.out.println(logMessage);
                    // LogUtils.writeLogToSubFolder("xml_parsing", "parsing_warning", logMessage);
                    if (isTagKey) {
                        tags.put(itemName, "-");
                    } else if (isFieldKey) {
                        fields.put(itemName, "-");
                    }
                    continue;
                }

                // SY_HOSTCMDCPNAME / SY_HOSTCMDCPVALUE 에 대한 처리 일단 주석 처리
                /*
                if (isTagKey) {
                    if (itemName.startsWith("SY_HOSTCMDCPNAME")) {
                        if (siteValue != null && !siteValue.isEmpty()) {
                            pendingHostCmdName = siteValue;
                        }
                        continue;
                    }
                    if (itemName.startsWith("SY_HOSTCMDCPVALUE")) {
                        tags.put(pendingHostCmdName, siteValue);
                        pendingHostCmdName = null;
                        continue;
                    }
                    tags.put(itemName, siteValue);
                    pendingHostCmdName = null;
                }
                */

                // tag키와 Field키 정리 -> tag키의 경우 무조껀 string으로 field키의 경우 value값에 따라 처리
                if (isTagKey) {
                    tags.put(itemName, siteValue);
                }
                if (isFieldKey) {
                    fields.put(itemName, parseValue(siteValue));
                }
            }
            System.out.println("here run");
            // influxTimeKey 방어 코드 + 시간 계산 코드
            if (influxTimeString == null || influxTimeString.isEmpty()) {
                System.out.println("Warning: " + timeKey + " no data. Skip InfluxDB store.");
                return;
            }
            long timestamp = parseInfluxTime(influxTimeString);
            System.out.println("timestamp=" + timestamp);
            // 혹시 모를 timestamp 값 현재 시간 세팅
            /*
            Long now  = System.currentTimeMillis();
            timestamp = now;
             */

            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("dbName", dbName);
            jsonMap.put("measurement", influxV3measurement);
            jsonMap.put("timestamp", (Object) timestamp);
            jsonMap.put("tags", tags);
            jsonMap.put("fields", fields);

            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(jsonMap);

            // Python 프로세스 실행
            ProcessBuilder pb = new ProcessBuilder(
                    "./python_influx_v3/python/python.exe",
                    "./python_influx_v3/influx_writer.py",
                    Main.influxV3Url,
                    Main.influxV3Token,
                    Main.influxV3Org
            );


            /* // JSON 파일 저장
            File jsonFile = new File("data.json");
            try (FileWriter writer = new FileWriter(jsonFile)) {
                writer.write("{\n");
                writer.write("\"dbName\": \"" + dbName + "\",\n");
                writer.write("\"measurement\": \"" + influxV3measurement + "\",\n");
                writer.write("\"timestamp\": " + timestamp + ",\n");
                writer.write("\"tags\": " + toJson(tags) + ",\n");
                writer.write("\"fields\": " + toJson(fields) + "\n");
                writer.write("}");
            }
            // Python 스크립트 실행 (InfluxDB v3 적재)
            ProcessBuilder pb = new ProcessBuilder(
                    "./python_influx_v3/python/python",
                    "./python_influx_v3/influx_writer.py",
                    jsonFile.getAbsolutePath(),
                    Main.influxV3Url,
                    Main.influxV3Token,
                    Main.influxV3Org
            );
            */

            // pb.inheritIO();
            Process p = pb.start();

            // JSON 문자열을 Python으로 전송
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(p.getOutputStream(), "UTF-8"))) {
                writer.write(jsonString);
                writer.flush();
                writer.close();
            }

            // Python stdout 읽기
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[Python stdout] " + line);
                }
            }

            p.waitFor(); // Python 실행 종료 대기
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Object parseValue(String value) {
        if (value == null || value.trim().isEmpty()) return null;
        try {
            if (value.contains(".")) return (Object) Double.parseDouble(value);
            return (Object) Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return value;
        }
    }

    private static long parseInfluxTime(String timeString) {
        try {
            if (timeString.length() != 16)
                throw new IllegalArgumentException("Unexpected time format: " + timeString);
            String datePart = timeString.substring(0, 8);
            String timePart = timeString.substring(8, 14);
            String milliPart = timeString.substring(14);
            String formatted = datePart + timePart;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            Date parsedDate = sdf.parse(formatted);
            long millis = parsedDate.getTime();
            int centi = Integer.parseInt(milliPart);
            millis += centi * 10;
            return millis;
        } catch (Exception e) {
            throw new RuntimeException("timestamp parsing error: " + timeString, e);
        }
    }

    private static String toJson(Map<?, ?> map) {
        StringBuilder json = new StringBuilder("{");
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            json.append("\"").append(entry.getKey()).append("\": ");
            Object val = entry.getValue();
            if (val == null) {
                json.append("null");
            } else if (val instanceof String) {
                json.append("\"").append(val).append("\"");
            } else {
                json.append(val);
            }
            json.append(", ");
        }
        if (json.length() > 1) json.setLength(json.length() - 2);
        json.append("}");
        return json.toString();
    }
}
