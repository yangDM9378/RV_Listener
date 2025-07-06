import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

public class Main {
    public static ConfigLoader config;

    // InfluxDB v3
    public static String influxV3Url;
    public static String influxV3Token;
    public static String influxV3Org;
    public static String[] influxV3Databases;

    // DB Key 정보
    public static Set<String> tagKeys;
    public static Set<String> fieldKeys;
    public static String timeKey;

    // Tibco RV 정보
    public static int tibcoRVPort;
    public static String tibcoRVNetwork;
    public static String tibcoRVDaemon;
    public static String[] tibcoRVSubjects;

    public static void main(String[] args) {
        // Config 정보 가져오기
        String configPath = "./TibcoRV_Listener.config";
        config = new ConfigLoader(configPath);

        // Influx v3
        influxV3Url       = config.getProperty("influxdb.v3.url");
        influxV3Token     = config.getProperty("influxdb.v3.token");
        influxV3Org       = config.getProperty("influxdb.v3.org");
        influxV3Databases = config.getProperty("influxdb.v3.databases").split(",");

        // DB 키 정보
        tagKeys  = new HashSet<>(Arrays.asList(config.getProperty("tag.keys").split(",")));
        fieldKeys = new HashSet<>(Arrays.asList(config.getProperty("field.keys").split(",")));
        timeKey   = config.getProperty("time.key");

        // Tibco RV 정보
        tibcoRVPort     = config.getIntProperty("tibco.rv.port", 7500);
        tibcoRVNetwork  = config.getProperty("tibco.rv.network");
        tibcoRVDaemon   = config.getProperty("tibco.rv.daemon");
        tibcoRVSubjects = config.getProperty("tibco.rv.subjects").split(",");


        // Tibco RV 연동
        try {
            TibcoRVListener rvListener = new TibcoRVListener(tagKeys, fieldKeys, timeKey);
            rvListener.start(tibcoRVNetwork, tibcoRVDaemon, tibcoRVPort, tibcoRVSubjects);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}