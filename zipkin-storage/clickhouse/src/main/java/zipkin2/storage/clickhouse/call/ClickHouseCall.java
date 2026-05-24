package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QuerySettings;
import zipkin2.Call;
import zipkin2.Callback;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public abstract class ClickHouseCall<V> extends Call.Base<V> {
  protected final Client client;
  protected final String database;
  private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(20);

  private final AtomicReference<Future<?>> pendingFuture = new AtomicReference<>();
  private volatile String activeQueryId;

  protected ClickHouseCall(Client client, String database) {
    this.client = client;
    this.database = database;
  }

  protected QuerySettings newQuerySettings() {
    String queryId = UUID.randomUUID().toString();
    activeQueryId = queryId;
    return new QuerySettings().setQueryId(queryId);
  }

  protected InsertSettings newInsertSettings() {
    String queryId = UUID.randomUUID().toString();
    activeQueryId = queryId;
    return new InsertSettings().setQueryId(queryId);
  }

  @Override
  protected final void doEnqueue(Callback<V> callback) {
    Future<?> future = EXECUTOR.submit(() -> {
      try {
        V result = doExecute();
        callback.onSuccess(result);
      } catch (Throwable e) {
        callback.onError(e);
      }
    });
    pendingFuture.set(future);
  }

  @Override
  protected void doCancel() {
    Future<?> future = pendingFuture.getAndSet(null);
    if (future != null) {
      future.cancel(true);
    }
    String queryId = activeQueryId;
    if (queryId != null) {
      try {
        client.execute("KILL QUERY WHERE query_id = '" + queryId + "' ASYNC").get();
      } catch (Exception ignored) {
      }
    }
  }

  @Override
  protected abstract V doExecute();
}
