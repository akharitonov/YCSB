package site.ycsb.db;

import dev.jcri.mdde.registry.clinet.tcp.benchmark.BenchmarkClient;
import dev.jcri.mdde.registry.server.tcp.Constants;
import dev.jcri.mdde.registry.shared.benchmark.IMDDEBenchmarkClient;
import dev.jcri.mdde.registry.shared.benchmark.commands.LocateTuple;
import dev.jcri.mdde.registry.shared.benchmark.commands.ReleaseCapacity;
import dev.jcri.mdde.registry.shared.benchmark.responses.TupleLocation;
import dev.jcri.mdde.registry.shared.benchmark.ycsb.MDDEClientConfiguration;
import dev.jcri.mdde.registry.shared.commands.containers.CommandResultContainer;
import dev.jcri.mdde.registry.shared.commands.containers.args.WriteArgsInsertContainer;
import redis.clients.jedis.*;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
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
  private IMDDEBenchmarkClient mddeBenchmarkClient = null;

  @Override
  protected void additionalConfiguration(MDDEClientConfiguration parsedConfig) throws DBException {
    // TODO: Better way of supplying config specifics to the client
    String host = parsedConfig.getRegistryNetworkConnection()
        .get(Constants.HOST_FIELD);
    String portString  = parsedConfig.getRegistryNetworkConnection()
        .get(Constants.PORT_CONTROL_FILED);
    int port = Integer.parseInt(portString);
    String benchPortString =parsedConfig.getRegistryNetworkConnection()
        .get(Constants.PORT_BENCHMARK_FIELD);
    int benchPort = Integer.parseInt(benchPortString);

    try {

      mddeRegistryClient = new MDDEClientTCP(
          host,
          port);
    } catch (Exception e) {
      throw new DBException("Failed to create a new MDDE TCP Client", e);
    }

    try {
      mddeBenchmarkClient = new BenchmarkClient(
          host,
          benchPort);
      mddeBenchmarkClient.openConnection();
    } catch (Exception e) {
      throw new DBException("Failed to create a new MDDE Benchmark TCP Client", e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    TupleLocation location = null;
    try {
      location = mddeBenchmarkClient.locateTuple(new LocateTuple(key));
      if(location == null || !location.tupleExists()){
        return Status.NOT_FOUND;
      }

      JedisPool pool = nodesPool.get(location.getNodeId());
      if(pool == null){
        if(verbose){
          System.err.println(
              String.format("READ ERROR: Unable to locate Redis connection with Id %s", location.getNodeId()));
        }
        return Status.ERROR;
      }

      try (Jedis jedis = pool.getResource()) {
        if (fields == null) {
          StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
        } else {
          String[] fieldArray = fields.toArray(new String[fields.size()]);
          List<String> values = jedis.hmget(key, fieldArray);

          for (int i = 0; i < fieldArray.length; i++) {
            String iField = fieldArray[i];
            String iVal = values.get(i);
            if (iVal != null) {
              result.put(iField, new StringByteIterator(iVal));
            }
          }
        }
      }
    } catch (InterruptedException e) {
      if(verbose){
        System.err.println(String.format("READ ERROR: %s", e.getMessage()));
      }
      return Status.ERROR;
    } finally {
      try {
        if(location != null){
          mddeBenchmarkClient.releaseCapacity(new ReleaseCapacity(location.getNodeId()));
        }
      } catch (Exception ex){
        System.err.println(ex.getMessage());
      }
    }
    boolean success = result.isEmpty();
    notifyRead(location.getNodeId(), key, success);
    return success ? Status.NOT_FOUND : Status.OK;
  }

  /**
   * Select the node for insertion.
   * @return Jedis instance.
   */
  @Override
  public String getNodeForInsertion() throws DBException {
    // Try to spread incoming insertions more-less uniformly across all nodes.
    Map<String, Long> currentStats = getDBCount();
    Map.Entry<String, Long> minRecordsNode = Collections.min(currentStats.entrySet(),
        Comparator.comparing(Map.Entry<String, Long>::getValue));
    return minRecordsNode.getKey();
  }

  @Override
  public Boolean confirmInsertion(String nodeId, String key) {
    try{
      WriteArgsInsertContainer cmdArgs = new WriteArgsInsertContainer();
      cmdArgs.setNodeId(nodeId);
      cmdArgs.setTupleId(key);
      CommandResultContainer<Boolean> response = mddeRegistryClient.insertTuple(cmdArgs);
      if(verbose){
        System.out.println(String.format("Confirm insertion of Key %s to Node %s. " +
            "Response: %b", key, nodeId, response.getResult()));
      }
      return response.getResult();
    }catch (Exception e){
      System.err.println("confirmInsertion error:" + e.getMessage());
      if(verbose){
        e.printStackTrace();
      }
      return false;
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
}
