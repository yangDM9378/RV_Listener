import org.w3c.dom.*;
import javax.xml.parsers.*;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class XmlParsingV3 {
    public static void parseXmlData(String xmlData, Set<String> tagKeys, Set<String> fieldKeys, String timeKey) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(xmlData.getBytes("UTF-8")));

            // messagename 검증
            NodeList messageNameList = doc.getElementsByTagName("messagename");
            if (messageNameList.getLength() == 0) {
                System.out.println("Error: no messagename tag, skip xml");
                return;
            }
            String messageName = messageNameList.item(0).getTextContent();
            if (!"TraceData".equals(messageName)) {
                System.out.println("Skip: messagename=" + messageName);
                return;
            }

            // MACHINENAME 추출 (DB 이름)
            NodeList dbNameList = doc.getElementsByTagName("MACHINENAME");
            if (dbNameList == null || dbNameList.getLength() == 0) {
                System.out.println("Skip: no MACHINENAME info");
                return;
            }
            String dbName = dbNameList.item(0).getTextContent();

            Map<String, Object> tags = new HashMap<>();
            Map<String, Object> fields = new HashMap<>();
            String influxTimeString = null;
            String pendingHostCmdName = null;

            NodeList items = doc.getElementsByTagName("ITEM");
            for (int i = 0; i < items.getLength(); i++) {
                Element item = (Element) items.item(i);
                String itemName = item.getElementsByTagName("ITEMNAME").item(0).getTextContent();

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
                    System.out.println("Skip: Config file의 Tag/Field 중복존재 " + itemName);
                    continue;
                }

                NodeList siteValueList = item.getElementsByTagName("SITEVALUE");
                if (siteValueList.getLength() != 1) {
                    System.out.println("Warning: SITEVALUE 없음. itemName=" + itemName);
                    continue;
                }

                String siteValue = siteValueList.item(0).getTextContent();

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

                if (isFieldKey) {
                    fields.put(itemName, parseValue(siteValue));
                }
            }

            if (influxTimeString == null || influxTimeString.isEmpty()) {
                System.out.println("Warning: " + timeKey + " no data. Skip InfluxDB store.");
                return;
            }

            long timestamp = parseInfluxTime(influxTimeString); // ← 시간 계산 포함됨

            // JSON 파일 저장
            File jsonFile = new File("data.json");
            try (FileWriter writer = new FileWriter(jsonFile)) {
                writer.write("{\n");
                writer.write("\"dbName\": \"" + dbName + "\",\n");
                writer.write("\"measurement\": \"Process\",\n");
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
            pb.inheritIO();
            Process p = pb.start();
            p.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Object parseValue(String value) {
        if (value == null || value.trim().isEmpty()) return null;
        try {
            if (value.contains(".")) return Double.parseDouble(value);
            return Integer.parseInt(value);
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
