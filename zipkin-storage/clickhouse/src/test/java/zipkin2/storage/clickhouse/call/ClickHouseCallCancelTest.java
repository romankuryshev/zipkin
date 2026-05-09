package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import zipkin2.Call;
import zipkin2.Callback;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.*;

class ClickHouseCallCancelTest {

  private final Client mockClient = mock(Client.class);

  private final class BlockingCall extends ClickHouseCall<String> {
    private final CountDownLatch queryStarted = new CountDownLatch(1);
    private final CountDownLatch releaseQuery;

    BlockingCall(CountDownLatch releaseQuery) {
      super(mockClient, "zipkin");
      this.releaseQuery = releaseQuery;
    }

    @Override
    protected String doExecute() {
      newQuerySettings();
      queryStarted.countDown();
      try {
        releaseQuery.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return "result";
    }

    @Override
    public Call<String> clone() {
      return new BlockingCall(new CountDownLatch(1));
    }

    @Override
    public String toString() {
      return "BlockingCall";
    }
  }

  private final class FastCall extends ClickHouseCall<String> {
    FastCall() {
      super(mockClient, "zipkin");
    }

    @Override
    protected String doExecute() {
      newQuerySettings();
      return "result";
    }

    @Override
    public Call<String> clone() {
      return new FastCall();
    }

    @Override
    public String toString() {
      return "FastCall";
    }
  }

  @Test
  void cancelSetsCanceledFlag() {
    var call = new FastCall();
    assertFalse(call.isCanceled());
    call.cancel();
    assertTrue(call.isCanceled());
  }

  @Test
  void newCall_isNotCanceled() {
    assertFalse(new FastCall().isCanceled());
  }

  @Test
  void cancelBeforeEnqueue_noKillQuerySent() {
    var call = new FastCall();
    call.cancel();
    verify(mockClient, never()).execute(anyString());
  }

  @Test
  void cancelBeforeEnqueue_enqueueDeliversError() throws InterruptedException {
    var call = new FastCall();
    call.cancel();

    CountDownLatch done = new CountDownLatch(1);
    AtomicBoolean gotError = new AtomicBoolean();

    call.enqueue(new Callback<>() {
      @Override public void onSuccess(String value) { done.countDown(); }
      @Override public void onError(Throwable t) {
        gotError.set(t instanceof IOException && "Canceled".equals(t.getMessage()));
        done.countDown();
      }
    });

    assertTrue(done.await(3, TimeUnit.SECONDS));
    assertTrue(gotError.get(), "Expected IOException(\"Canceled\") from callback");
  }

  @Test
  void cancelBeforeExecute_throwsIOException() {
    var call = new FastCall();
    call.cancel();
    assertThrows(IOException.class, call::execute);
  }

  @Test
  @Timeout(5)
  void cancelAfterQueryStarted_sendsKillQuery() throws InterruptedException {
    CountDownLatch release = new CountDownLatch(1);
    var call = new BlockingCall(release);

    call.enqueue(new Callback<>() {
      @Override public void onSuccess(String value) {}
      @Override public void onError(Throwable t) {}
    });

    assertTrue(call.queryStarted.await(5, TimeUnit.SECONDS), "doExecute() did not start in time");
    call.cancel();
    release.countDown();

    verify(mockClient).execute(contains("KILL QUERY WHERE query_id ="));
  }

  @Test
  @Timeout(5)
  void cancelAfterQueryStarted_marksCallAsCanceled() throws InterruptedException {
    CountDownLatch release = new CountDownLatch(1);
    var call = new BlockingCall(release);

    call.enqueue(new Callback<>() {
      @Override public void onSuccess(String value) {}
      @Override public void onError(Throwable t) {}
    });

    assertTrue(call.queryStarted.await(5, TimeUnit.SECONDS));
    call.cancel();
    release.countDown();

    assertTrue(call.isCanceled());
  }

  @Test
  @Timeout(5)
  void cancelAfterQueryStarted_pendingFutureIsNulledOut() throws InterruptedException {
    CountDownLatch release = new CountDownLatch(1);
    var call = new BlockingCall(release);

    call.enqueue(new Callback<>() {
      @Override public void onSuccess(String value) {}
      @Override public void onError(Throwable t) {}
    });

    assertTrue(call.queryStarted.await(5, TimeUnit.SECONDS));
    call.cancel();
    release.countDown();

    assertTrue(call.isCanceled());
    assertDoesNotThrow(call::cancel);
  }

  @Test
  void cancelIsIdempotent_neverThrows() {
    var call = new FastCall();
    assertDoesNotThrow(() -> {
      call.cancel();
      call.cancel();
    });
    assertTrue(call.isCanceled());
  }

  @Test
  @Timeout(5)
  void cancelIsIdempotent_killQuerySentEachTime() throws InterruptedException {
    CountDownLatch release = new CountDownLatch(1);
    var call = new BlockingCall(release);

    call.enqueue(new Callback<>() {
      @Override public void onSuccess(String value) {}
      @Override public void onError(Throwable t) {}
    });

    assertTrue(call.queryStarted.await(5, TimeUnit.SECONDS));
    call.cancel();
    call.cancel();
    release.countDown();

    verify(mockClient, times(2)).execute(contains("KILL QUERY WHERE query_id ="));
  }
}
