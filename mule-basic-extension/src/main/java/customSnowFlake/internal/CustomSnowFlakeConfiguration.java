package customSnowFlake.internal;

import org.mule.runtime.extension.api.annotation.Operations;
import org.mule.runtime.extension.api.annotation.connectivity.ConnectionProviders;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import java.util.*;

/**
 * This class represents an extension configuration, values set in this class
 * are commonly used across multiple
 * operations since they represent something core from the extension.
 */
@Operations(CustomSnowFlakeOperations.class)
@ConnectionProviders(CustomSnowFlakeConnectionProvider.class)
public class CustomSnowFlakeConfiguration {

  @Parameter
  private String user;

  public String getUser() {
    return user;
  }

  /**
   * A parameter that is always required to be configured.
   */
  @Parameter
  private String clientId;

  public String getClientId() {
    return clientId;
  }

  @Parameter
  private String clientSecret;

  public String getClientSecret() {
    return clientSecret;
  }

  @Parameter
  private String scope;

  public String getScope() {
    return scope;
  }

  @Parameter
  private String account;

  public String getAccount() {
    return account;
  }

  @Parameter
  private String warehouse;

  public String getWarehouse() {
    return warehouse;
  }

  @Parameter
  private String database;

  public String getDatabase() {
    return database;
  }

  @Parameter
  private String schema;

  public String getSchema() {
    return schema;
  }

  @Parameter
  private int multiStatementCount;

  public int getmultiStatementCount() {
    return multiStatementCount;
  }

}
