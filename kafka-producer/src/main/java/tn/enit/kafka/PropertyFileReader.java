package tn.enit.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFileReader {
    private static Properties prop = new Properties();

    public static Properties readPropertyFile() throws Exception {
        if (prop.isEmpty()) {
            // Load the properties file from the resources directory
            InputStream input = PropertyFileReader.class.getClassLoader().getResourceAsStream("kafka-producer.properties");
            try {
                if (input == null) {
                    throw new IOException("Unable to find application.properties");
                }
                prop.load(input);
            } catch (IOException ex) {
                System.out.println("Error reading properties file: " + ex.toString());
                throw ex;
            } finally {
                if (input != null) {
                    input.close();
                }
            }
        }
        return prop;
    }
}
