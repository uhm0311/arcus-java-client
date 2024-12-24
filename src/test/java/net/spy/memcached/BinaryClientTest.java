package net.spy.memcached;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test assumes a binary server is running on localhost:11211.
 */
class BinaryClientTest extends ProtocolBaseCase {

  @Override
  protected void initClient() throws Exception {
    initClient(new BinaryConnectionFactory() {
      @Override
      public long getOperationTimeout() {
        return 15000;
      }

      @Override
      public FailureMode getFailureMode() {
        return FailureMode.Retry;
      }
    });
  }

  @Override
  protected String getExpectedVersionSource() {
    return "localhost/" + ARCUS_HOST;
  }

  @Test
  @Override
  void testGetStatsCacheDump() throws Exception {
    // XXX:  Cachedump isn't returning anything from the server in binprot
    assertTrue(true);
  }

  @Test
  @Override
  void testGetsBulk() throws Exception {
    assertTrue(true);
  }

  @Test
  @Override
  void testAsyncGetsBulkWithTranscoderIterator() throws Exception {
    assertTrue(true);
  }

  @Test
  void testCASAppendFail() throws Exception {
    final String key = "append.key";
    assertTrue(client.set(key, 5, "test").get());
    CASValue<Object> casv = client.gets(key);
    assertFalse(client.append(casv.getCas() + 1, key, "es").get());
    assertEquals("test", client.get(key));
  }

  @Test
  void testCASAppendSuccess() throws Exception {
    final String key = "append.key";
    assertTrue(client.set(key, 5, "test").get());
    CASValue<Object> casv = client.gets(key);
    assertTrue(client.append(casv.getCas(), key, "es").get());
    assertEquals("testes", client.get(key));
  }

  @Test
  void testCASPrependFail() throws Exception {
    final String key = "append.key";
    assertTrue(client.set(key, 5, "test").get());
    CASValue<Object> casv = client.gets(key);
    assertFalse(client.prepend(casv.getCas() + 1, key, "es").get());
    assertEquals("test", client.get(key));
  }

  @Test
  void testCASPrependSuccess() throws Exception {
    final String key = "append.key";
    assertTrue(client.set(key, 5, "test").get());
    CASValue<Object> casv = client.gets(key);
    assertTrue(client.prepend(casv.getCas(), key, "es").get());
    assertEquals("estest", client.get(key));
  }
}
