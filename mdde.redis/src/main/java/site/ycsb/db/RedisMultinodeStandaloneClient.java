package site.ycsb.db;

import dev.jcri.mdde.registry.shared.benchmark.ycsb.MDDEClientConfiguration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Client relying on randomized insertions across nodes, works without the MDDE registry.
 * Intended for internal testing.
 */
public class RedisMultinodeStandaloneClient extends BaseRedisMultinodeClient {

  private Random randomNodeGen = new Random();

  @Override
  protected void additionalConfiguration(MDDEClientConfiguration parsedConfig) throws DBException {
    // No additional configuration is needed
  }

  @Override
  public String getNodeForInsertion(){
    // Randomly grabbing an instance from the pool of Redis DB nodes
    Object[] allIds =  nodesPool.keySet().toArray();
    return (String)allIds[randomNodeGen.nextInt(allIds.length)];
  }

  @Override
  public Boolean confirmInsertion(String nodeId, String key) {
    return true;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String node = null;
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
      if(!result.isEmpty()){
        node = entry.getKey();
        break;
      }
    }
    boolean success = result.isEmpty();
    notifyRead(node != null? node : "not_found", key, success);
    return success ? Status.NOT_FOUND : Status.OK;
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
