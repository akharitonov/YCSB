package site.ycsb.db;

import site.ycsb.DBException;

import java.util.Collections;
import java.util.List;

/**
 * Aggregate DBException.
 */
public class DBExceptionMDDEAggregate extends DBException {
  private List<Throwable> aggregateCauses;
  public DBExceptionMDDEAggregate(List<Throwable> causes) {
    super();
    aggregateCauses = Collections.unmodifiableList(causes);
  }

  private String generateAggregateMessage(List<Throwable> causes, boolean localized){
    if(causes == null || causes.size() == 0){
      return "Error";
    }
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < aggregateCauses.size(); i++) {
      Throwable cause = aggregateCauses.get(i);
      if(!localized){
        stringBuilder.append(cause.getMessage());
      } else {
        stringBuilder.append(cause.getLocalizedMessage());
      }
      if(i < aggregateCauses.size() -1) {
        stringBuilder.append(";\n");
      }
    }
    return stringBuilder.toString();
  }

  @Override
  public String getMessage() {
    return generateAggregateMessage(aggregateCauses, false);
  }

  @Override
  public String getLocalizedMessage() {
    return generateAggregateMessage(aggregateCauses, true);
  }

  @Override
  public synchronized Throwable getCause() {
    if (aggregateCauses == null || aggregateCauses.size() == 0) {
      return super.getCause();
    } else {
      return aggregateCauses.get(0);
    }
  }

  /**
   * Get all of the causes for this error.
   * @return Read only List of causes.
   */
  public synchronized List<Throwable> getCauses() {
    return aggregateCauses;
  }
}
