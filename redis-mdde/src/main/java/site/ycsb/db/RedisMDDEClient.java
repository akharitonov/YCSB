package site.ycsb.db;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import site.ycsb.db.config.RedisMDDEClientConfig;
import site.ycsb.db.config.RedisMDDEClientNode;

import java.io.Closeable;
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
  private Map<String, JedisCommands> nodesPool = new HashMap<>();

  private Random randomNodeGen = new Random();
  /**
   * Property flag containing path to the YAML config.
   */
  private static final String CONFIG_PATH = "redismdde.configfile";

  public static final String INDEX_KEY = "_indices";

  public void init() throws DBException {
    Properties props = getProperties();
    final String configPath = props.getProperty(CONFIG_PATH);
    File configFile = new File(configPath);
    if(!configFile.exists() || configFile.isDirectory()){
      // Somehow inappropriate exception but the one imposed by the superclass
      throw new DBException("Unable to find the configuration file provided " + configPath);
    }

    StringBuilder contentBuilder = new StringBuilder();
    try (Stream<String> stream = Files.lines(Paths.get(configPath), StandardCharsets.UTF_8)){
      stream.forEach(s -> contentBuilder.append(s).append("\n"));
    }
    catch (IOException e){
      e.printStackTrace();
    }
    String configText = contentBuilder.toString();

    final YAMLMapper mapper = new YAMLMapper();
    RedisMDDEClientConfig config;
    try {
      config = mapper.readValue(configText, RedisMDDEClientConfig.class);
    } catch (IOException e) {
      throw new DBException("Unable to parse the config file: " + e.getMessage());
    }

    for (RedisMDDEClientNode node : config.getNodes()){
      String host = node.getHost();
      int port = node.getPort();
      JedisCommands jedis = null;
      if (node.getCluster()) {
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        jedisClusterNodes.add(new HostAndPort(host, port));
        jedis = new JedisCluster(jedisClusterNodes);
      } else {
        jedis = new Jedis(host, port);
      }
      nodesPool.put(node.getNodeId(), jedis);
      if (node.getPassword() != null) {
        ((BasicCommands) jedis).auth(Arrays.toString(node.getPassword()));
      }
      ((Jedis)jedis).connect();
    }
  }

  /**
   * Close all connections to Redis Db instances in the pool.
   * @throws DBException
   */
  public void cleanup() throws DBException {
    try {
      for (JedisCommands jedis: nodesPool.values()){
        ((Closeable) jedis).close();
      }
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
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
    nodesPool.forEach((k, jedis) -> {
        if (fields == null) {
          StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
        } else {
          String[] fieldArray = (String[]) fields.toArray(new String[fields.size()]);
          List<String> values = jedis.hmget(key, fieldArray);
          Iterator<String> fieldIterator = fields.iterator();
          Iterator<String> valueIterator = values.iterator();

          while (fieldIterator.hasNext() && valueIterator.hasNext()) {
            result.put(fieldIterator.next(),
                new StringByteIterator(valueIterator.next()));
          }
          assert !fieldIterator.hasNext() && !valueIterator.hasNext();
        }
      });

    return result.isEmpty() ? Status.ERROR : Status.OK;
  }


  @Override
  public Status insert(String table, String key,
                      Map<String, ByteIterator> values) {
    // Randomly grabbing an instance from the pool of RedisDbs
    // TODO: Refactor and \ or change logic
    Object[] allJedis = nodesPool.values().toArray();
    JedisCommands jedis = (JedisCommands) allJedis[randomNodeGen.nextInt(allJedis.length)];

    if (jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK")) {
      jedis.zadd(INDEX_KEY, hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    HashMap<String, Status> result = new HashMap<>();
    nodesPool.forEach((k, jedis) ->{
        result.put(k, jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR : Status.OK);
      });

    return result.containsValue(Status.OK) ? Status.OK : Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    HashMap<String, Status> result = new HashMap<>();
    nodesPool.forEach((k, jedis) ->{
        result.put(k, jedis.hmset(key,
            StringByteIterator.getStringMap(values)).equals("OK")
            ? Status.OK
            : Status.ERROR);
      });

    return result.containsValue(Status.OK) ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startKey, int recordCount,
                      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    for (JedisCommands jedis: nodesPool.values()) {
      Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startKey),
          Double.POSITIVE_INFINITY, 0, recordCount);

      HashMap<String, ByteIterator> values;
      for (String key : keys) {
        values = new HashMap<String, ByteIterator>();
        read(table, key, fields, values);
        result.add(values);
      }
    }
    return Status.OK;
  }
}
