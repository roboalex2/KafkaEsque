package at.esque.kafka;

import at.esque.kafka.serialization.logicaltypes.KafkaEsqueConversions;
import ch.qos.logback.classic.ClassicConstants;

import java.nio.file.Files;
import java.nio.file.Path;

public class Launcher {

    static {
        Path logbackFile = Path.of(System.getProperty("user.home"), ".kafkaesque", "logback.xml");
        if (Files.isRegularFile(logbackFile)) {
            System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, logbackFile.toAbsolutePath().toString());
        }
        KafkaEsqueConversions.load();
    }

    public static void main(String[] args) {
        Main.main(args);
    }

}
