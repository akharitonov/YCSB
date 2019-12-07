package site.ycsb.db.config;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSetter;

/**
 * Configuration pointing to the Redis instance running in the network
 */
@JsonPropertyOrder({ RedisMDDEClientNode.ID_FIELD,
                      RedisMDDEClientNode.HOST_FIELD,
                      RedisMDDEClientNode.PORT_FIELD,
                      RedisMDDEClientNode.USERNAME_FIELD,
                      RedisMDDEClientNode.PASSWORD_FIELD,
                      RedisMDDEClientNode.CLUSTER_FIELD })
public class RedisMDDEClientNode {
  public static final String ID_FIELD = "id";
  public static final String PORT_FIELD = "port";
  public static final String USERNAME_FIELD = "username";
  public static final String PASSWORD_FIELD = "password";
  public static final String HOST_FIELD = "host";
  public static final String CLUSTER_FIELD = "host";

  private String _nodeId;
  private String _host = null;
  private Integer _port = 6379;
  private char[] _username = null;
  private char[] _password = null;
  private Boolean _isCluster = false;

  /**
   * Get ID of the node
   * @return
   */
  @JsonGetter(RedisMDDEClientNode.ID_FIELD)
  public String getNodeId(){
    return _nodeId;
  }

  /**
   * Set ID of the node
   * @param nodeId
   */
  @JsonSetter(RedisMDDEClientNode.ID_FIELD)
  public void setNodeId(String nodeId){
    _nodeId = nodeId;
  }

  /**
   * Get HOST name or the IP address of the Redis node
   * @return
   */
  @JsonProperty(RedisMDDEClientNode.HOST_FIELD)
  public String getHost() {
    return _host;
  }

  /**
   * Set HOST name or the IP address of the Redis node
   * @param _host
   */
  @JsonSetter(RedisMDDEClientNode.HOST_FIELD)
  public void setHost(String _host) {
    this._host = _host;
  }

  /**
   * Get port of the Redis node
   * @return
   */
  @JsonProperty(RedisMDDEClientNode.PORT_FIELD)
  public Integer getPort() {
    return _port;
  }

  /**
   * Set port of the Redis node
   * @param _port
   */
  @JsonSetter(RedisMDDEClientNode.PORT_FIELD)
  public void setPort(Integer _port) {
    this._port = _port;
  }

  /**
   * Get username of the Redis node
   * @return
   */
  @JsonProperty(RedisMDDEClientNode.USERNAME_FIELD)
  public char[] getUsername() {
    return _username;
  }

  /**
   * Set username of the Redis node if required
   * @param _username
   */
  @JsonSetter(RedisMDDEClientNode.USERNAME_FIELD)
  public void setUsername(char[] _username) {
    this._username = _username;
  }

  /**
   * Get password of the Redis node
   * @return
   */
  @JsonProperty(RedisMDDEClientNode.PASSWORD_FIELD)
  public char[] getPassword() {
    return _password;
  }

  /**
   * Set password of the Redis node if required
   * @param _password
   */
  @JsonSetter(RedisMDDEClientNode.PASSWORD_FIELD)
  public void setPassword(char[] _password) {
    this._password = _password;
  }

  /**
   * Determine if the node is a cluster node
   * @return True - is cluster
   */
  @JsonProperty(RedisMDDEClientNode.CLUSTER_FIELD)
  public Boolean getCluster() {
    return _isCluster;
  }
  @JsonSetter(RedisMDDEClientNode.CLUSTER_FIELD)
  public void setCluster(Boolean cluster) {
    this._isCluster = cluster;
  }
}
