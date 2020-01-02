package site.ycsb.db.config;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSetter;

/**
 * Configuration pointing to the Redis instance running in the network.
 */
@JsonPropertyOrder({ RedisMDDEClientNode.ID_FIELD,
                      RedisMDDEClientNode.HOST_FIELD,
                      RedisMDDEClientNode.PORT_FIELD,
                      RedisMDDEClientNode.PASSWORD_FIELD})
public class RedisMDDEClientNode {
  public static final String ID_FIELD = "id";
  public static final String PORT_FIELD = "port";
  public static final String PASSWORD_FIELD = "password";
  public static final String HOST_FIELD = "host";

  private String nodeId;
  private String host = null;
  private Integer port = 6379;
  private char[] password = null;

  /**
   * Get ID of the node.
   * @return Redis node assigned Id within the MDDE registry.
   */
  @JsonGetter(RedisMDDEClientNode.ID_FIELD)
  public String getNodeId(){
    return nodeId;
  }

  /**
   * Set ID of the node.
   * @param nId Redis node assigned Id within the MDDE registry.
   */
  @JsonSetter(RedisMDDEClientNode.ID_FIELD)
  public void setNodeId(String nId){
    this.nodeId = nId;
  }

  /**
   * Get HOST name or the IP address of the Redis node.
   * @return Redis node host.
   */
  @JsonProperty(RedisMDDEClientNode.HOST_FIELD)
  public String getHost() {
    return host;
  }

  /**
   * Set HOST name or the IP address of the Redis node.
   * @param redisHost Redis node host.
   */
  @JsonSetter(RedisMDDEClientNode.HOST_FIELD)
  public void setHost(String redisHost) {
    this.host = redisHost;
  }

  /**
   * Get port of the Redis node.
   * @return Redis node port.
   */
  @JsonProperty(RedisMDDEClientNode.PORT_FIELD)
  public Integer getPort() {
    return port;
  }

  /**
   * Set port of the Redis node.
   * @param redisPort Redis node port.
   */
  @JsonSetter(RedisMDDEClientNode.PORT_FIELD)
  public void setPort(Integer redisPort) {
    this.port = redisPort;
  }

  /**
   * Get password of the Redis node.
   * @return Redis node password.
   */
  @JsonProperty(RedisMDDEClientNode.PASSWORD_FIELD)
  public char[] getPassword() {
    return password;
  }

  /**
   * Set password of the Redis node if required.
   * @param redisPassword Redis node password.
   */
  @JsonSetter(RedisMDDEClientNode.PASSWORD_FIELD)
  public void setPassword(char[] redisPassword) {
    this.password = redisPassword;
  }
}
