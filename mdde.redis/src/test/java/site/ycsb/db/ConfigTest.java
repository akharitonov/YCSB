package site.ycsb.db;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import dev.jcri.mdde.registry.shared.benchmark.ycsb.MDDEClientConfiguration;

import org.junit.Test;
import static org.junit.Assert.assertEquals;


import java.io.IOException;


public class ConfigTest {

  @Test
  public void testDeserializationSerialization() {
    final String sampleYAMLConfig =
        "---\n" +
        "mddeHost: \"localhost\"\n" +
        "mddePort: 4285\n" +
        "nodes:\n" +
        "  - id: \"mddednode1\"\n" +
        "    host: \"localhost\"\n" +
        "    port: 6479\n" +
        "    password: null\n" +
        "  - id: \"mddednode2\"\n" +
        "    host: \"localhost\"\n" +
        "    port: 6579\n" +
        "    password: null\n";
    System.out.println(sampleYAMLConfig);
    final YAMLMapper mapper = new YAMLMapper();
    MDDEClientConfiguration config = null;
    try {
      config = mapper.readValue(sampleYAMLConfig, MDDEClientConfiguration.class);
    } catch (IOException e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
    assertEquals(2, config.getNodes().size());
    assertEquals(sampleYAMLConfig, config.toString());
  }
}
