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
   * Insert tuple into the registry.
   * @param arguments
   * @return
   */
  CommandResultContainer<String> insertTuple(WriteArgsInsertContainer arguments) throws IOException;

  /**
   * Delete tuple from the registry.
   * @param arguments
   * @return
   */
  CommandResultContainer<String> deleteTuple(WriteArgsDeleteTupleContainer arguments);
}
