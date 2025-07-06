import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogUtils {
    public static void writeLogToSubFolder(String subFolderName, String filePrefix, String content) {
        try {
            File logDir = new File("./Log/" + subFolderName);
            if (!logDir.exists()) {
                logDir.mkdirs();  // ✔️ 하위 폴더까지 모두 생성
            }

            String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS").format(new Date());
            String filename = "./Log/" + subFolderName + "/" + filePrefix + "_" + timeStamp + ".log";

            try (FileWriter writer = new FileWriter(filename)) {
                writer.write(content);
                System.out.println("[LogUtils] 로그 저장됨: " + filename);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
