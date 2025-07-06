import java.util.Set;
import com.tibco.tibrv.*;

public class TibcoRVListener implements TibrvMsgCallback {
    private TibrvTransport transport;
    private Set<String> tagKeys;
        private Set<String> fieldKeys;
        private String timeKey;
        private String influxV3Database;
        private String influxV3measurement;

    public TibcoRVListener(Set<String> tagKeys, Set<String> fieldKeys, String timeKey, String influxV3Database, String influxV3measurement) {
            this.tagKeys = tagKeys;
            this.fieldKeys = fieldKeys;
            this.timeKey = timeKey;
            this.influxV3Database = influxV3Database;
            this.influxV3measurement = influxV3measurement;
    }

    public void start(String network, String daemon, int servicePort, String subject) throws TibrvException {
        Tibrv.open(Tibrv.IMPL_NATIVE);
        transport = new TibrvRvdTransport(String.valueOf(servicePort), network, daemon);
        new TibrvListener(Tibrv.defaultQueue(), this, transport, subject, null);
        System.out.println("Listening on subject: " + subject);
        // subject 여러개 가정
//        for (String subject : subjects) {
//            new TibrvListener(Tibrv.defaultQueue(), this, transport, subject, null);
//            System.out.println("Listening on subject: " + subject);
//        }
        new TibrvDispatcher(Tibrv.defaultQueue());
    }

    @Override
    public void onMsg(TibrvListener listener, TibrvMsg message) {
        try {
//            String fullMessage = message.toString();
//            LogUtils.writeLogToSubFolder("tibcoRV_message", "tibco_message", fullMessage);

            TibrvMsgField field = message.getField("xmlData");
            if (field != null && field.data instanceof String) {
                String xmlData = (String) field.data;

                System.out.println("[TibcoRVListener] sending xmlData: " + xmlData);
//                LogUtils.writeLogToSubFolder("xml_parsing", "xmlData", xmlData);
                XmlParsingV3.parseXmlData(xmlData, tagKeys, fieldKeys, timeKey, influxV3Database, influxV3measurement);

            } else {
                System.out.println("[TibcoRVListener] no xmlData field all data: " + message.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}