package site.ycsb.db;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import site.ycsb.db.config.RedisMDDEClientConfig;

import org.junit.Test;
import static org.junit.Assert.assertEquals;


import java.io.IOException;


public class ConfigTest {

  @Test
  public void testDeserializationSerialization() {
    final String sampleYAMLConfig =
        "---\n" +
        "nodes:\n" +
        "- id: \"TestNode 1\"\n" +
          "  host: \"localhost\"\n" +
          "  port: 6379\n" +
          "  password: null\n"+
          "  cluster: false\n"+
        "- id: \"TestNode 2\"\n" +
          "  host: \"192.168.0.2\"\n" +
          "  port: 6379\n" +
          "  password: \"some_password\"\n"+
          "  cluster: false\n";
    System.out.println(sampleYAMLConfig);
    final YAMLMapper mapper = new YAMLMapper();
    RedisMDDEClientConfig config = null;
    try {
      config = mapper.readValue(sampleYAMLConfig, RedisMDDEClientConfig.class);
    } catch (IOException e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
    assertEquals(2, config.getNodes().size());
    assertEquals(sampleYAMLConfig, config.toString());
  }
}
