package site.ycsb.db.config;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.IOException;
import java.util.List;

/**
 * Client configuration containing information needed to connect to any.
 */
public class RedisMDDEClientConfig {
  @JsonIgnore
  private List<RedisMDDEClientNode> redisMDDEClientNodes;

  /**
   * Redis Instances.
   * @return Redis data nodes configurations.
   */
  @JsonGetter("nodes")
  public List<RedisMDDEClientNode> getNodes() {
    return redisMDDEClientNodes;
  }

  public void setNodes(List<RedisMDDEClientNode> redisNodes){
    redisMDDEClientNodes = redisNodes;
  }

  @Override
  public String toString() {
    YAMLMapper mapper = new YAMLMapper();
    try {
      return mapper.writeValueAsString(this);
    } catch (IOException e) {
      return null;
    }
  }
}

