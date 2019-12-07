package site.ycsb.db.config;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.IOException;
import java.util.List;

/**
 * Client configuration containing information needed to connect to any
 */
public class RedisMDDEClientConfig {
  @JsonIgnore
  private List<RedisMDDEClientNode> _RedisMDDEClient_nodes;

  /**
   * Redis Instances
   * @return
   */
  @JsonGetter("nodes")
  public List<RedisMDDEClientNode> getNodes() {
    return _RedisMDDEClient_nodes;
  }

  public void setNodes(List<RedisMDDEClientNode> redisMDDEClientNodes){
    _RedisMDDEClient_nodes = redisMDDEClientNodes;
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

