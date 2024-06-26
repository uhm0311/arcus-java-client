package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.transcoders.Transcoder;

public class TestConnectionFactoryBuilder extends ConnectionFactoryBuilder {

  private Collection<ConnectionObserver> observers = Collections.emptyList();
  private final ConnectionFactory inner;

  public TestConnectionFactoryBuilder(ConnectionFactory cf) {
    this.inner = cf;
  }

  public static TestConnectionFactoryBuilder CreateDefault() {
    return new TestConnectionFactoryBuilder(new DefaultConnectionFactory());
  }

  @Override
  public ConnectionFactory build() {
    return new ConnectionFactory() {
      /*
       * CAUTION! Never override this createConnection() method, or your code could not
       * work as you expected. See https://github.com/jam2in/arcus-java-client/issue/4
       */
      @Override
      public MemcachedConnection createConnection(
              String name, List<InetSocketAddress> addrs) throws IOException {
        return new MemcachedConnection(name, this, addrs,
                getInitialObservers(), getFailureMode(), getOperationFactory());
      }

      @Override
      public MemcachedNode createMemcachedNode(String name,
                                               SocketAddress sa,
                                               int bufSize) {
        return inner.createMemcachedNode(name, sa, bufSize);
      }

      @Override
      public BlockingQueue<Operation> createOperationQueue() {
        return inner.createOperationQueue();
      }

      @Override
      public BlockingQueue<Operation> createReadOperationQueue() {
        return inner.createReadOperationQueue();
      }

      @Override
      public BlockingQueue<Operation> createWriteOperationQueue() {
        return inner.createWriteOperationQueue();
      }

      @Override
      public long getOpQueueMaxBlockTime() {
        return inner.getOpQueueMaxBlockTime();
      }

      @Override
      public NodeLocator createLocator(List<MemcachedNode> nodes) {
        return inner.createLocator(nodes);
      }

      @Override
      public OperationFactory getOperationFactory() {
        return inner.getOperationFactory();
      }

      @Override
      public long getOperationTimeout() {
        return inner.getOperationTimeout();
      }

      @Override
      public boolean isDaemon() {
        return inner.isDaemon();
      }

      @Override
      public boolean useNagleAlgorithm() {
        return inner.useNagleAlgorithm();
      }

      @Override
      public boolean getKeepAlive() {
        return inner.getKeepAlive();
      }

      @Override
      public boolean getDnsCacheTtlCheck() {
        return inner.getDnsCacheTtlCheck();
      }

      @Override
      public Collection<ConnectionObserver> getInitialObservers() {
        return Stream.concat(inner.getInitialObservers().stream(), observers.stream())
                .collect(Collectors.toList());
      }

      @Override
      public FailureMode getFailureMode() {
        return inner.getFailureMode();
      }

      @Override
      public Transcoder<Object> getDefaultTranscoder() {
        return inner.getDefaultTranscoder();
      }

      @Override
      public Transcoder<Object> getDefaultCollectionTranscoder() {
        return inner.getDefaultCollectionTranscoder();
      }

      @Override
      public boolean shouldOptimize() {
        return inner.shouldOptimize();
      }

      @Override
      public int getReadBufSize() {
        return inner.getReadBufSize();
      }

      @Override
      public HashAlgorithm getHashAlg() {
        return inner.getHashAlg();
      }

      @Override
      public long getMaxReconnectDelay() {
        return inner.getMaxReconnectDelay();
      }

      @Override
      public AuthDescriptor getAuthDescriptor() {
        return inner.getAuthDescriptor();
      }

      @Override
      public int getTimeoutExceptionThreshold() {
        return inner.getTimeoutExceptionThreshold();
      }

      @Override
      public int getTimeoutRatioThreshold() {
        return inner.getTimeoutRatioThreshold();
      }

      @Override
      public int getTimeoutDurationThreshold() {
        return inner.getTimeoutDurationThreshold();
      }

      @Override
      public int getMaxFrontCacheElements() {
        return inner.getMaxFrontCacheElements();
      }

      @Override
      public String getFrontCacheName() {
        return inner.getFrontCacheName();
      }

      @Override
      public boolean getFrontCacheCopyOnRead() {
        return inner.getFrontCacheCopyOnRead();
      }

      @Override
      public boolean getFrontCacheCopyOnWrite() {
        return inner.getFrontCacheCopyOnWrite();
      }

      @Override
      public int getFrontCacheExpireTime() {
        return inner.getFrontCacheExpireTime();
      }

      @Override
      public int getDefaultMaxSMGetKeyChunkSize() {
        return inner.getDefaultMaxSMGetKeyChunkSize();
      }

      @Override
      public byte getDelimiter() {
        return inner.getDelimiter();
      }

      /* ENABLE_REPLICATION if */

      @Override
      public ReadPriority getReadPriority() {
        return inner.getReadPriority();
      }

      @Override
      public Map<APIType, ReadPriority> getAPIReadPriority() {
        return inner.getAPIReadPriority();
      }
      /* ENABLE_REPLICATION end */
    };
  }

  @Override
  public ConnectionFactoryBuilder setInitialObservers(Collection<ConnectionObserver> obs) {
    this.observers = obs;
    return this;
  }
}
