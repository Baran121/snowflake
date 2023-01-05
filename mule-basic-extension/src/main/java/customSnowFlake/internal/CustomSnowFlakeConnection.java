package customSnowFlake.internal;

/**
 * This class represents an extension connection just as example (there is no
 * real connection with anything here c:).
 */
public final class CustomSnowFlakeConnection {

  private final CustomSnowFlakeConfiguration config;

  public CustomSnowFlakeConnection(CustomSnowFlakeConfiguration config) {
    this.config = config;
  }

  public CustomSnowFlakeConfiguration getConfig() {
    return config;
  }

  public void invalidate() {
    // do something to invalidate this connection!
  }
}
