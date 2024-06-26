/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2022 JaM2in Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached;

import java.util.ArrayList;
import java.util.Collection;

import junit.framework.TestCase;

public abstract class ClientBaseCase extends TestCase {

  public static final String ZK_HOST = System.getProperty("ZK_HOST",
          "127.0.0.1:2181");

  public static final String ZK_SERVICE_ID = System.getProperty("ZK_SERVICE_ID",
          "test");

  public static final String ARCUS_HOST = System
          .getProperty("ARCUS_HOST",
                  "127.0.0.1:11211");

  public static final boolean USE_ZK = Boolean.valueOf(System.getProperty(
          "USE_ZK", "false"));

  public static Collection<String> stringify(Collection<?> c) {
    Collection<String> rv = new ArrayList<>();
    for (Object o : c) {
      rv.add(String.valueOf(o));
    }
    return rv;
  }

  protected static boolean SHUTDOWN_AFTER_EACH_TEST = USE_ZK;

  static {
    LoggerSetter.setLog4JLogger();

    System.out.println("---------------------------------------------");
    System.out.println("[ArcusClient initialization info.]");
    System.out.println("USE_ZK=" + USE_ZK);
    System.out.println("SHUTDOWN_AFTER_EACH_TEST=" + USE_ZK);
    if (USE_ZK) {
      System.out.println("ZK_HOST=" + ZK_HOST + ", ZK_SERVICE_ID="
              + ZK_SERVICE_ID);
    } else {
      System.out.println("ARCUS_HOST=" + ARCUS_HOST);
    }
    System.out.println("---------------------------------------------");
  }

  protected MemcachedClient client = null;

  protected void initClient() throws Exception {
    initClient(new DefaultConnectionFactory() {
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

  protected void initClient(ConnectionFactory cf) throws Exception {
    ConnectionFactoryBuilder cfb = new TestConnectionFactoryBuilder(cf);

    if (USE_ZK) {
      openFromZK(cfb);
    } else {
      openDirect(cfb);
    }
  }

  protected void openFromZK(ConnectionFactoryBuilder cfb) {
    client = ArcusClient.createArcusClient(ZK_HOST, ZK_SERVICE_ID, cfb);
  }

  protected void openDirect(ConnectionFactoryBuilder cfb) throws Exception {
    client = ArcusClient.createArcusClient(cfb, AddrUtil.getAddresses(ARCUS_HOST));
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    initClient();
  }

  @Override
  protected void tearDown() throws Exception {
    // Shut down, start up, flush, and shut down again. Error tests have
    // unpredictable timing issues.
    client.shutdown();
    client = null;
    initClient();
    flushPause();
    assertTrue(client.flush().get());
    client.shutdown();
    client = null;
    super.tearDown();
  }

  protected void flushPause() throws InterruptedException {
    // nothing useful
  }

}
