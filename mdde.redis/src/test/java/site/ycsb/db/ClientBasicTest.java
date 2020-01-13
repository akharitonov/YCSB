package site.ycsb.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.jcri.mdde.registry.shared.benchmark.ycsb.MDDEClientConfiguration;
import dev.jcri.mdde.registry.shared.benchmark.ycsb.MDDEClientConfigurationReader;
import org.junit.Test;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class ClientBasicTest {

  @Test
  public void writeReadDeleteSingle() throws DBException {
    InputStream is = ClientBasicTest.class.getResourceAsStream("/test-config.yml");
    String yamlTestConfig = new BufferedReader(new InputStreamReader(is)).lines()
        .parallel().collect(Collectors.joining("\n"));

    RedisMultinodeMDDEClient testClient = new RedisMultinodeMDDEClient();
    MDDEClientConfigurationReader mddeClientConfigReader = new MDDEClientConfigurationReader();
    MDDEClientConfiguration configuration = null;
    try {
      configuration = mddeClientConfigReader.deserializeConfiguration(yamlTestConfig);
    } catch (JsonProcessingException e) {
      fail(e.getMessage());
    }
    testClient.initWithMDDEClientConfig(configuration);
    testClient.flush(false);

    String key = "k_1";
    String field = "f_1";
    String val = "test_value_1";
    Map<String, ByteIterator> values = new HashMap<String, ByteIterator>(){
      {put(field, new StringByteIterator(val));}
    };

    Status insertResult = testClient.insert(null, key, values);
    assertEquals(Status.OK, insertResult);

    Map<String, ByteIterator> result = new HashMap<String, ByteIterator>(); //
    Status readResult = testClient.read(null, key, values.keySet(), result);
    assertEquals(Status.OK, readResult);
    assertEquals(1, result.size());
    assertEquals(val, result.get(field).toString());

    Status deleteResult = testClient.delete(null, key);
    assertEquals(Status.OK, deleteResult);
    result.clear();
  }

}
