import java.util.Set;
import com.tibco.tibrv.*;

public class TibcoRVListener implements TibrvMsgCallback {
    private TibrvTransport transport;
    private Set<String> tagKeys;
    private Set<String> fieldKeys;
    private String timeKey;

    public TibcoRVListener(Set<String> tagKeys, Set<String> fieldKeys, String timeKey) {
        this.tagKeys = tagKeys;
        this.fieldKeys = fieldKeys;
        this.timeKey = timeKey;
    }

    public void start(String network, String daemon, int servicePort, String[] subjects) throws TibrvException {
        Tibrv.open(Tibrv.IMPL_NATIVE);
        transport = new TibrvRvdTransport(String.valueOf(servicePort), network, daemon);

        for (String subject : subjects) {
            new TibrvListener(Tibrv.defaultQueue(), this, transport, subject, null);
            System.out.println("Listening on subject: " + subject);
        }

        new TibrvDispatcher(Tibrv.defaultQueue());
    }

    @Override
    public void onMsg(TibrvListener listener, TibrvMsg message) {
        try {
            TibrvMsgField field = message.getField("xmlData");
            if (field != null) {
                String xmlData = (String) field.data;
                System.out.println("[TibcoRVListener] 수신한 xmlData: " + xmlData);

                XmlParsingV3.parseXmlData(xmlData, tagKeys, fieldKeys, timeKey);

            } else {
                System.out.println("[TibcoRVListener] xmlData 필드가 없습니다. 전체 메시지: " + message.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}