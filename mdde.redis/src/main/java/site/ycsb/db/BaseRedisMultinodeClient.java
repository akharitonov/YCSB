package site.ycsb.db;

import dev.jcri.mdde.registry.shared.benchmark.ycsb.MDDEClientConfiguration;
import dev.jcri.mdde.registry.shared.benchmark.ycsb.MDDEClientConfigurationReader;
import dev.jcri.mdde.registry.shared.configuration.DBNetworkNodesConfiguration;
import redis.clients.jedis.*;
import site.ycsb.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * Base abstract class for implementing benchmarks  that work with multiple Redis instances but don't rely on the
 * built-in Redis DB clusters. Instead we supply our own distribution control and retrieval logic.
 */
public abstract class BaseRedisMultinodeClient extends DB {
  /**
   * Pool of RedisDB connected nodes.
   */
  protected Map<String, JedisPool> nodesPool = new HashMap<>();
  /**
   * Verbosity flag, use for triggering debug logs.
   */
  protected boolean verbose = false;

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
    MDDEClientConfigurationReader mddeClientConfigReader = new MDDEClientConfigurationReader();
    MDDEClientConfiguration configuration = null;
    try {
      configuration = mddeClientConfigReader.readConfiguration(Paths.get(configPath));
    } catch (IOException e) {
      throw new DBException("Failed to read the config file", e);
    }

    initWithMDDEClientConfig(configuration);

    if(nodesPool.size() == 0) {
      throw new DBException("Data nodes are't specified.");
    }
  }

  /**
   * Initialized this instance with the textual configuration.
   * @param config Parsed MDDE client config file.
   * @throws DBException Error of the configuration.
   */
  public void initWithMDDEClientConfig(MDDEClientConfiguration config) throws DBException{
    Objects.requireNonNull(config);
    for (DBNetworkNodesConfiguration node : config.getNodes()){
      if(!node.getDefaultNode()){
        continue;
      }

      String host = node.getHost();
      int port = node.getPort();

      JedisPoolConfig configPool = new JedisPoolConfig();
      JedisPool nodeCPool = null;

      if(verbose){
        System.out.println(String.format("Configure Redis connection pool:\n\tHost: %s\n\tPort: %d", host, port));
      }

      if (node.getPassword() != null) {
        if(verbose){
          System.out.println(String.format("Password is set for Redis connection pool:\n\tHost: %s\n\tPort: %d",
              host, port));
        }
        nodeCPool = new JedisPool(configPool, host, port,  2000, new String(node.getPassword()));
      } else {
        nodeCPool = new JedisPool(configPool, host, port,  2000);
      }
      if(verbose){
        // Attempt to connect
        try(Jedis jedis = nodeCPool.getResource()){
          String pong = jedis.ping();
          System.out.println(String.format("Redis response to PING: %s", pong));
        }
        System.out.println(String.format("Redis Node %s is open: %b", node.getNodeId(), !nodeCPool.isClosed()));
      }
      nodesPool.put(node.getNodeId(), nodeCPool);
    }
    if(verbose){
      System.out.println(String.format("Nodes added %d", nodesPool.size()));
    }

    // Do any additional implementation specific configuration
    additionalConfiguration(config);
  }

  /**
   * Implement this method to do any additional configuration required for a specific implementation.
   * @param parsedConfig Parsed RedisMDDEClientConfig, not null.
   */
  protected abstract void additionalConfiguration(MDDEClientConfiguration parsedConfig) throws DBException;

  /**
   * Close all connections to Redis Db instances in the pool.
   * @throws DBException DBExceptionMDDEAggregate.
   */
  @Override
  public void cleanup() throws DBException {
    List<Throwable> errors = null;
    for (String poolId : nodesPool.keySet()){
      if(verbose){
        System.out.println(String.format("Closing pool for node: %s", poolId));
      }
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
  protected double hash(String key) {
    return key.hashCode();
  }
  // TODO: Better hash

  @Override
  public abstract Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result);

  /**
   * Get a count of records on every node.
   * @return Map NodeId : number of records.
   * @throws DBException DBExceptionMDDEAggregate.
   */
  protected Map<String, Long> getDBCount() throws DBException {
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    Map<String, Long> result = new ConcurrentHashMap<>();
    for(String nodeId: nodesPool.keySet()){
      result.put(nodeId, (long) -1);
    }
    nodesPool.keySet().parallelStream().forEach(poolId -> {
        try {
          try(Jedis jedis = nodesPool.get(poolId).getResource()) {
            result.put(poolId, jedis.dbSize());
          }
        } catch (Exception e) {
          errors.add(new DBException(String.format("Failed fetching DBSIZE for node %s.", poolId)));
        }
      });
    if(errors.size() > 0){
      throw new DBExceptionMDDEAggregate(errors);
    }
    return result;
  }

  /**
   * Select the node for insertion.
   * @return Jedis instance.
   */
  public abstract String getNodeForInsertion() throws DBException;

  /**
   * Add the key to MDDE registry or make any other manipulations for post insertion.
   * If returned false, the inserted record is removed.
   * @param key Inserted key
   * @return True - proceed with the insertion. False - roll insertion back
   */
  public abstract Boolean confirmInsertion(String nodeId, String key);

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String nodeId = null;
    try {
      nodeId = getNodeForInsertion();
    } catch (DBException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    if(verbose){
      System.out.println(String.format("INSERT Key: %s to Pool: %s (Open: %b)",
          key,
          nodeId,
          !nodesPool.get(nodeId).isClosed()));
    }
    try(Jedis jedis = nodesPool.get(nodeId).getResource()) {
      Map<String, String> strValuesMap = StringByteIterator.getStringMap(values);
      if(verbose){
        System.out.println(String.format("Inserting key %s, num values: %d", key, values.size()));
      }
      long nSetFields = jedis.hset(key, strValuesMap);
      if (nSetFields == values.size()) {
        if(!confirmInsertion(nodeId, key)){
          if(verbose){
            System.out.println(String.format("INSERT key: %s to pool: %s is NOT confirmed. Rolling back",
                key,
                nodeId));
          }
          jedis.del(key);
          return Status.ERROR;
        }
        jedis.zadd(INDEX_KEY, hash(key), key);
        return Status.OK;
      } else {
        return Status.ERROR;
      }
    } catch (Exception e) {
      if(verbose) {
        System.err.println("INSERT ERROR: " + e.getMessage());
        if(e.getCause() != null){
          System.err.println(e.getCause().getMessage());
        }
      }
      return Status.ERROR;
    }
  }

  @Override
  public abstract Status delete(String table, String key);

  @Override
  public abstract Status update(String table, String key, Map<String, ByteIterator> values);

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
