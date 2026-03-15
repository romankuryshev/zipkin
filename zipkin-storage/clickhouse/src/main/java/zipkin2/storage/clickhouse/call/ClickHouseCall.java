package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.Callback;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public abstract class ClickHouseCall<V> extends Call.Base<V> {
  protected final Client client;
  protected final String database;
  private static final Executor EXECUTOR = ForkJoinPool.commonPool();

  protected ClickHouseCall(Client client, String database) {
    this.client = client;
    this.database = database;
  }

  @Override
  protected final void doEnqueue(Callback<V> callback) {
    EXECUTOR.execute(() -> {
      try {
        V result = doExecute();
        callback.onSuccess(result);
      } catch (Throwable e) {
        callback.onError(e);
      }
    });
  }

  @Override
  protected abstract V doExecute();
}
