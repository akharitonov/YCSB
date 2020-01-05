package site.ycsb.db.mdde;

import dev.jcri.mdde.registry.shared.commands.containers.CommandResultContainer;
import dev.jcri.mdde.registry.shared.commands.containers.args.WriteArgsDeleteTupleContainer;
import dev.jcri.mdde.registry.shared.commands.containers.args.WriteArgsInsertContainer;

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

  /**
   * Insert tuple into the registry
   * @param arguments
   * @return
   */
  CommandResultContainer<String> InsertTuple(WriteArgsInsertContainer arguments);

  /**
   * Delete tuple from the registry
   * @param arguments
   * @return
   */
  CommandResultContainer<String> DeleteTuple(WriteArgsDeleteTupleContainer arguments);
}
