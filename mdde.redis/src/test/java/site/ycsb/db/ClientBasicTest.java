package site.ycsb.db;

import org.junit.Test;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ClientBasicTest {

  @Test
  public void writeReadDeleteSingle() throws DBException {
    InputStream is = ClientBasicTest.class.getResourceAsStream("/test-config.yml");
    String yamlTestConfig = new BufferedReader(new InputStreamReader(is)).lines()
        .parallel().collect(Collectors.joining("\n"));

    RedisMDDEClient testClient = new RedisMDDEClient();
    testClient.initWithTextConfig(yamlTestConfig);
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

  @Test
  public void writeReadDeleteMassParallel() throws DBException {
    Properties props = new Properties();
    InputStream is = ClientBasicTest.class.getResourceAsStream("/test-config.yml");
    String yamlTestConfig = new BufferedReader(new InputStreamReader(is)).lines()
        .parallel().collect(Collectors.joining("\n"));

    RedisMDDEClient testClient = new RedisMDDEClient();
    testClient.initWithTextConfig(yamlTestConfig);
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
