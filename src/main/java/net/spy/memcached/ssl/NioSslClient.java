package net.spy.memcached.ssl;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManagerFactory;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.compat.SpyObject;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus;

public final class NioSslClient extends SpyObject {

  private final String remoteAddress;
  private final int port;

  private final SSLEngine engine;
  private final SocketChannel socketChannel;

  private ByteBuffer myNetData;
  private ByteBuffer peerAppData;
  private ByteBuffer peerNetData;
  private final Queue<Byte> readBuffer;

  public NioSslClient(InetSocketAddress isa, ConnectionFactory connFactory) throws IOException {
    remoteAddress = isa.getHostName();
    port = isa.getPort();

    SSLContext context;
    try {
      String keystorePassword = "";

      KeyStore keyStore = KeyStore.getInstance("PKCS12");
      try (FileInputStream fis = new FileInputStream("/Users/uhm0311/server.p12")) {
        keyStore.load(fis, keystorePassword.toCharArray());
      }

      KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
              KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(keyStore, keystorePassword.toCharArray());

      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
              TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(keyStore);

      context = SSLContext.getInstance("TLS");
      context.init(keyManagerFactory.getKeyManagers(),
                   trustManagerFactory.getTrustManagers(),
                   null);
    } catch (Exception e) {
      throw new IOException(e);
    }
    engine = context.createSSLEngine(remoteAddress, port);
    engine.setUseClientMode(true);

    SSLSession session = engine.getSession();
    int appBufferSize = session.getApplicationBufferSize();
    int netBufferSize = session.getPacketBufferSize();

    myNetData = ByteBuffer.allocate(netBufferSize);
    peerAppData = ByteBuffer.allocate(appBufferSize);
    peerNetData = ByteBuffer.allocate(netBufferSize);
    readBuffer = new LinkedList<>();

    socketChannel = SocketChannel.open();
    socketChannel.configureBlocking(false);
    socketChannel.socket().setTcpNoDelay(!connFactory.useNagleAlgorithm());
    socketChannel.socket().setKeepAlive(connFactory.getKeepAlive());
    socketChannel.socket().setReuseAddress(true);
  }

  public boolean connect() throws IOException {
    return socketChannel.connect(new InetSocketAddress(remoteAddress, port));
  }

  public boolean finishConnect() throws IOException {
    if (!socketChannel.finishConnect()) {
      return false;
    }

    engine.beginHandshake();
    return doHandshake(socketChannel, engine);
  }

  public int write(ByteBuffer buffer) throws IOException {
    if (!buffer.hasRemaining()) {
      return 0;
    }

    final int position = buffer.position();
    myNetData.clear();

    SSLEngineResult result = engine.wrap(buffer, myNetData);
    switch (result.getStatus()) {
      case OK:
        myNetData.flip();
        while (myNetData.hasRemaining()) {
          socketChannel.write(myNetData);
        }

        System.out.println("writing: " + new String(buffer.array(), position, buffer.position() - position));
        return buffer.position() - position;
      case BUFFER_OVERFLOW:
        myNetData = enlargePacketBuffer(myNetData);
        return 0;
      case BUFFER_UNDERFLOW:
        throw new SSLException("Buffer underflow occured after a wrap. " +
                "I don't think we should ever get here.");
      case CLOSED:
        close();
        buffer.clear();
        return -1;
      default:
        throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
    }
  }

  private String toString(ByteBuffer b, int length) {
    StringBuilder str = new StringBuilder();
    byte[] arr = b.array();
    int l = Math.min(b.remaining(), length);

    for (int i = b.position(); i < l; i++) {
      if (arr[i] != 0) {
        str.append((char) arr[i]);
      }
    }

    return str.toString();
  }

  private String toString(ByteBuffer b) {
    return toString(b, b.remaining());
  }

  public int read(ByteBuffer buffer) throws IOException {
    if (!buffer.hasRemaining()) {
      return 0;
    }
    if (!readBuffer.isEmpty()) {
      int size = Math.min(buffer.remaining(), readBuffer.size());
      byte[] data = new byte[size];

      for (int i = 0; i < size; i++) {
        data[i] = readBuffer.poll();
      }

      System.out.println("remain raw: " + Arrays.toString(data));
      System.out.println("remain: " + new String(data));
      buffer.put(data);
      return size;
    }
    peerNetData.compact();

    boolean loop = true;
    int read = 0;

    while (loop) {
      int bytesRead = socketChannel.read(peerNetData);
      if (bytesRead == 0 && !peerNetData.hasRemaining()) {
        return 0;
      }
      if (bytesRead < 0) {
        handleEndOfStream();
        return bytesRead;
      }

      peerNetData.flip();
      while (peerNetData.hasRemaining()) {
        SSLEngineResult result = engine.unwrap(peerNetData, peerAppData);
        switch (result.getStatus()) {
          case OK:
            peerAppData.flip();
            read += transferByteBuffer(peerAppData, buffer);
            loop = false;

            if (!peerAppData.hasRemaining()) {
              peerAppData.clear();
              break;
            }
            if (!buffer.hasRemaining()) {
              while (peerAppData.hasRemaining()) {
                readBuffer.offer(peerAppData.get());
              }
              peerAppData.clear();
              return read;
            }
            break;
          case BUFFER_OVERFLOW:
            peerAppData = enlargeApplicationBuffer(peerAppData);
            break;
          case BUFFER_UNDERFLOW:
            peerNetData.compact();
            peerNetData = handleBufferUnderflow(peerNetData);
            return read;
          case CLOSED:
            close();
            return -1;
          default:
            throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
        }
      }

      peerNetData.clear();
    }

    if (!peerAppData.hasRemaining()) {
      peerAppData.clear();
    }
    return read;
  }

  public void close() throws IOException {
    engine.closeOutbound();
    doHandshake(socketChannel, engine);
    socketChannel.close();
  }

  public boolean isOpen() {
    return socketChannel.isOpen();
  }

  public boolean isConnected() {
    return socketChannel.isConnected();
  }

  public boolean isConnectionPending() {
    return socketChannel.isConnectionPending();
  }

  public Socket socket() {
    return socketChannel.socket();
  }

  public SelectionKey register(Selector sel, int ops, Object att) throws ClosedChannelException {
    return socketChannel.register(sel, ops, att);
  }

  private boolean isHandshakeDone(HandshakeStatus status) {
    return status == HandshakeStatus.FINISHED || status == HandshakeStatus.NOT_HANDSHAKING;
  }

  private boolean doHandshake(SocketChannel socketChannel, SSLEngine engine) throws IOException {
    SSLEngineResult result;
    HandshakeStatus handshakeStatus;

    int appBufferSize = engine.getSession().getApplicationBufferSize();
    ByteBuffer myAppData = ByteBuffer.allocate(appBufferSize);
    ByteBuffer peerAppData = ByteBuffer.allocate(appBufferSize);
    myNetData.clear();
    peerNetData.clear();

    handshakeStatus = engine.getHandshakeStatus();
    while (!isHandshakeDone(handshakeStatus)) {
      switch (handshakeStatus) {
        case NEED_UNWRAP:
          if (socketChannel.read(peerNetData) < 0) {
            if (engine.isInboundDone() && engine.isOutboundDone()) {
              return false;
            }
            try {
              engine.closeInbound();
            } catch (SSLException e) {
              getLogger().error("This engine was forced to close inbound," +
                      "without having received the proper SSL/TLS " +
                      "close notification message from the peer, " +
                      "due to end of stream.", e);
            }
            engine.closeOutbound();
            handshakeStatus = engine.getHandshakeStatus();
            break;
          }
          peerNetData.flip();
          try {
            result = engine.unwrap(peerNetData, peerAppData);
            peerNetData.compact();
            handshakeStatus = result.getHandshakeStatus();
          } catch (SSLException e) {
            getLogger().error("A problem was encountered while processing the data" +
                    "that caused the SSLEngine to abort. " +
                    "Will try to properly close connection...", e);
            engine.closeOutbound();
            handshakeStatus = engine.getHandshakeStatus();
            break;
          }
          switch (result.getStatus()) {
            case OK:
              break;
            case BUFFER_OVERFLOW:
              peerAppData = enlargeApplicationBuffer(peerAppData);
              break;
            case BUFFER_UNDERFLOW:
              peerNetData = handleBufferUnderflow(peerNetData);
              break;
            case CLOSED:
              if (engine.isOutboundDone()) {
                return false;
              } else {
                engine.closeOutbound();
                handshakeStatus = engine.getHandshakeStatus();
                break;
              }
            default:
              throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
          }
          break;
        case NEED_WRAP:
          myNetData.clear();
          try {
            result = engine.wrap(myAppData, myNetData);
            handshakeStatus = result.getHandshakeStatus();
          } catch (SSLException e) {
            getLogger().error("A problem was encountered while processing the data" +
                    "that caused the SSLEngine to abort." +
                    "Will try to properly close connection...", e);
            engine.closeOutbound();
            handshakeStatus = engine.getHandshakeStatus();
            break;
          }
          switch (result.getStatus()) {
            case OK :
              myNetData.flip();
              while (myNetData.hasRemaining()) {
                socketChannel.write(myNetData);
              }
              break;
            case BUFFER_OVERFLOW:
              myNetData = enlargePacketBuffer(myNetData);
              break;
            case BUFFER_UNDERFLOW:
              throw new SSLException("Buffer underflow occured after a wrap. " +
                      "I don't think we should ever get here.");
            case CLOSED:
              try {
                myNetData.flip();
                while (myNetData.hasRemaining()) {
                  socketChannel.write(myNetData);
                }

                peerNetData.clear();
              } catch (IOException e) {
                getLogger().error("Failed to send server's CLOSE message " +
                        "due to socket channel's failure.", e);
                handshakeStatus = engine.getHandshakeStatus();
              }
              break;
            default:
              throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
          }
          break;
        case NEED_TASK:
          Runnable task;
          while ((task = engine.getDelegatedTask()) != null) {
            task.run();
          }
          handshakeStatus = engine.getHandshakeStatus();
          break;
        default:
          throw new IllegalStateException("Invalid SSL status: " + handshakeStatus);
      }
    }

    return true;
  }

  private ByteBuffer enlargePacketBuffer(ByteBuffer buffer) {
    return enlargeBuffer(buffer, engine.getSession().getPacketBufferSize());
  }

  private ByteBuffer enlargeApplicationBuffer(ByteBuffer buffer) {
    return enlargeBuffer(buffer, engine.getSession().getApplicationBufferSize());
  }

  private ByteBuffer enlargeBuffer(ByteBuffer buffer, int sessionProposedCapacity) {
    if (sessionProposedCapacity > buffer.capacity()) {
      buffer = ByteBuffer.allocate(sessionProposedCapacity);
    } else {
      buffer = ByteBuffer.allocate(buffer.capacity() * 2);
    }
    return buffer;
  }

  private ByteBuffer handleBufferUnderflow(ByteBuffer buffer) {
    if (engine.getSession().getPacketBufferSize() < buffer.limit()) {
      return buffer;
    } else {
      ByteBuffer replaceBuffer = enlargePacketBuffer(buffer);
      buffer.flip();
      replaceBuffer.put(buffer);
      return replaceBuffer;
    }
  }

  private void handleEndOfStream() throws IOException {
    try {
      engine.closeInbound();
    } catch (SSLException e) {
      getLogger().error("This engine was forced to close inbound, " +
              "without having received the proper SSL/TLS " +
              "close notification message from the peer, " +
              "due to end of stream.", e);
    }
    close();
  }

  private int transferByteBuffer(ByteBuffer from, ByteBuffer to) {
    int fremain = from.remaining();
    int toremain = to.remaining();
    if (fremain > toremain) {
      System.out.println("reading (fremain > toremain): " + toString(from, toremain));
      int i = 0;
      for (; i < toremain; i++) {
        to.put(from.get());
      }
      //System.out.println("last value of from: " + from.get(toremain - 1));
      return toremain;
    } else {
      if (from.limit() > 0) {
        //System.out.println("last value of from: " + from.get(fremain - 1));
      }
      System.out.println("reading (fremain <= toremain): " + toString(from));
      to.put(from);
      return fremain;
    }
  }
}
