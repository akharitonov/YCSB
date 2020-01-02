package site.ycsb.db.mdde;

import java.io.IOException;

/**
 * Base interface for the MDDE Client connection.
 */
public interface IMDDEClient extends AutoCloseable {
  /**
   * Send query and get a response.
   * @param command JSON command.
   * @return JSON response.
   */
  String sendCommand(String command) throws IOException;
}
