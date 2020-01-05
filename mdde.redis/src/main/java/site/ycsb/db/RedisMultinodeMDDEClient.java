package site.ycsb.db;

import redis.clients.jedis.*;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.config.RedisMDDEClientConfig;
import site.ycsb.db.mdde.IMDDEClient;
import site.ycsb.db.mdde.MDDEClientTCP;

import java.util.*;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a> nodes in <a href="https://github.com/jcridev/mdde">MDDE</a>.
 * Relies on the MDDE registry for distribution control.
 * See {@code redis-mdde/README.md} for details.
 */
public class RedisMultinodeMDDEClient extends BaseRedisMultinodeClient {

  private IMDDEClient mddeRegistryClient = null;

  @Override
  protected void additionalConfiguration(RedisMDDEClientConfig parsedConfig) throws DBException {
    try {
      mddeRegistryClient = new MDDEClientTCP(parsedConfig.getMddeRegistryHost(), parsedConfig.getMddeRegistryPort());
    } catch (Exception e) {
      throw new DBException("Failed to create a new MDDE TCP Client", e);
    }
  }

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

  /**
   * Select the node for insertion.
   * @return Jedis instance.
   */
  @Override
  public Jedis getNodeForInsertion() throws DBException {
    // Try to spread incoming insertions more-less uniformly across all nodes.
    Map<String, Long> currentStats = getDBCount();
    Map.Entry<String, Long> minRecordsNode = Collections.min(currentStats.entrySet(),
        Comparator.comparing(Map.Entry<String, Long>::getValue));
    return nodesPool.get(minRecordsNode.getKey()).getResource();
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
}
