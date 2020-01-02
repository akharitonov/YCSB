package site.ycsb.db;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import redis.clients.jedis.*;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.config.RedisMDDEClientConfig;
import site.ycsb.db.config.RedisMDDEClientNode;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a> node in <a href="https://github.com/jcridev/mdde">MDDE</a>.
 *
 * See {@code redis-mdde/README.md} for details.
 */
public class RedisMDDEClient extends DB {
  /**
   * Pool of RedisDB connected nodes.
   */
  private Map<String, JedisPool> nodesPool = new HashMap<>();

  private boolean verbose = false;

  private Random randomNodeGen = new Random();
  /**
   * Property flag containing path to the YAML config.
   */
  private static final String CONFIG_PATH = "mdde.redis.configfile";
  private static final String VERBOSE_P = "verbose";
  public static final String INDEX_KEY = "_indices";

  public void init() throws DBException {
    Properties props = getProperties();
    final String configPath = props.getProperty(CONFIG_PATH);
    File configFile = new File(configPath);
    if(!configFile.exists() || configFile.isDirectory()){
      // Somehow inappropriate exception but the one imposed by the superclass
      throw new DBException("Unable to find the configuration file provided " + configPath);
    }

    if ((getProperties().getProperty(VERBOSE_P) != null) &&
        (getProperties().getProperty(VERBOSE_P).compareTo("true") == 0)) {
      verbose = true;
    }

    StringBuilder contentBuilder = new StringBuilder();
    try (Stream<String> stream = Files.lines(Paths.get(configPath), StandardCharsets.UTF_8)){
      stream.forEach(s -> contentBuilder.append(s).append("\n"));
    }
    catch (IOException e){
      throw new DBException("Failed to read the config file", e);
    }
    String configText = contentBuilder.toString();
    initWithTextConfig(configText);

    if(nodesPool.size() == 0) {
      throw new DBException("Data nodes are't specified.");
    }
  }

  public void initWithTextConfig(String configText) throws DBException{
    final ObjectMapper mapper = new YAMLMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    RedisMDDEClientConfig config;
    try {
      config = mapper.readValue(configText, RedisMDDEClientConfig.class);
    } catch (IOException e) {
      throw new DBException("Unable to parse the config file: " + e.getMessage());
    }

    for (RedisMDDEClientNode node : config.getNodes()){
      String host = node.getHost();
      int port = node.getPort();

      JedisPoolConfig configPool = new JedisPoolConfig();
      JedisPool nodeCPool = null;
      if (node.getPassword() != null) {
        nodeCPool = new JedisPool(configPool, host, port,  2000, new String(node.getPassword()));
      } else {
        nodeCPool = new JedisPool(configPool, host, port,  2000);
      }
      nodesPool.put(node.getNodeId(), nodeCPool);
    }
  }

  /**
   * Close all connections to Redis Db instances in the pool.
   * @throws DBException DBExceptionMDDEAggregate.
   */
  public void cleanup() throws DBException {
    List<Throwable> errors = null;
    for (String poolId : nodesPool.keySet()){
      try {
        nodesPool.get(poolId).close();
      } catch (Exception e) {
        if(errors == null){
          errors = new LinkedList<>();
        }
        errors.add(new DBException(String.format("Closing connection failed for node %s.", poolId)));
      }
    }
    if(errors != null){
      throw new DBExceptionMDDEAggregate(errors);
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    for (Map.Entry<String, JedisPool> entry : nodesPool.entrySet()) {
      JedisPool pool = entry.getValue();
      try (Jedis jedis = pool.getResource()) {
        if (fields == null) {
          StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
        } else {
          String[] fieldArray = fields.toArray(new String[fields.size()]);
          List<String> values = jedis.hmget(key, fieldArray);

          for(int i = 0; i < fieldArray.length; i++){
            String iField = fieldArray[i];
            String iVal = values.get(i);
            if(iVal != null) {
              result.put(iField, new StringByteIterator(iVal));
            }
          }
        }
      }
    }

    return result.isEmpty() ? Status.NOT_FOUND : Status.OK;
  }


  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    // Randomly grabbing an instance from the pool of RedisDbs
    // TODO: Refactor and \ or change logic
    Object[] allJedisPools = nodesPool.values().toArray();
    JedisPool randJedisPool = (JedisPool) allJedisPools[randomNodeGen.nextInt(allJedisPools.length)];
    try(Jedis jedis = randJedisPool.getResource()) {
      Map<String, String> strValuesMap = StringByteIterator.getStringMap(values);
      if(verbose){
        System.out.println(String.format("Inserting key %s, num values: %d", key, values.size()));
      }
      long nSetFields = jedis.hset(key, strValuesMap);
      if (nSetFields == values.size()) {
        jedis.zadd(INDEX_KEY, hash(key), key);
        return Status.OK;
      } else {
        return Status.ERROR;
      }
    }
  }

  @Override
  public Status delete(String table, String key) {
    Status result = Status.ERROR;
    for (Map.Entry<String, JedisPool> entry : nodesPool.entrySet()) {
      JedisPool jedisPool = entry.getValue();
      try (Jedis jedis = jedisPool.getResource()) {
        jedis.watch(key);
        Transaction t = jedis.multi();
        Response<Long> removedKeys = t.del(key);
        Response<Long> removedIndices = t.zrem(INDEX_KEY, key);
        t.exec();

        if (removedKeys.get() > 0 && removedIndices.get() > 0) {
          result = Status.OK;
          break;
        }
      }
    }

    return result;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    for (Map.Entry<String, JedisPool> entry : nodesPool.entrySet()) {
      JedisPool jedisPool = entry.getValue();
      try (Jedis jedis = jedisPool.getResource()) {
        boolean existsInTheNode = jedis.exists(key);
        if (!existsInTheNode) {
          continue;
        }
        Map<String, String> stringValues = StringByteIterator.getStringMap(values);
        long nSetValues = jedis.hset(key, stringValues);
        if(verbose) {
          System.out.println(
              String.format("UPDATE: set for key %S. Added fields %d, supplied values %d",
                  key, nSetValues, values.size()));
        }
        return Status.OK;
      }
    }
    if(verbose) {
      String notFoundKey = String.format("UPDATE: Failed to find the key: %S", key);
      System.out.println(notFoundKey);
    }
    return Status.NOT_FOUND;
  }

  @Override
  public Status scan(String table, String startKey, int recordCount,
                      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    for (JedisPool jedisPool: nodesPool.values()) {
      Set<String> keys = null;
      try(Jedis jedis = jedisPool.getResource()) {
        keys = jedis.zrangeByScore(INDEX_KEY, hash(startKey),
            Double.POSITIVE_INFINITY, 0, recordCount);
      }
      HashMap<String, ByteIterator> values;
      for (String key : keys) {
        values = new HashMap<String, ByteIterator>();
        read(table, key, fields, values);
        result.add(values);
      }
    }
    return Status.OK;
  }

  /**
   * Flush all of the keys from all of the nodes.
   * @param andClose If True, also close the connection pools.
   * @throws DBExceptionMDDEAggregate DBExceptionMDDEAggregate.
   */
  public void flush(boolean andClose) throws DBExceptionMDDEAggregate {
    List<Throwable> errors = null;
    for (String poolId : nodesPool.keySet()){
      try {
        JedisPool currentPool = nodesPool.get(poolId);
        currentPool.getResource().flushAll();
        if(andClose) {
          currentPool.close();
        }
      } catch (Exception e) {
        if(errors == null){
          errors = new LinkedList<>();
        }
        errors.add(new DBException(String.format("Flushing failed connection failed for node %s.", poolId)));
      }
    }
    if(errors != null){
      throw new DBExceptionMDDEAggregate(errors);
    }
  }
}
