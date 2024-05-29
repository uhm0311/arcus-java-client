package net.spy.memcached;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.ops.DeleteOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.StoreOperation;

public final class TimedOutMessageFactory {

  private TimedOutMessageFactory() {
  }

  public static String createTimedoutMessage(long beforeAwait,
                                             long duration,
                                             TimeUnit units,
                                             Operation op) {

    return createTimedoutMessage(beforeAwait, duration, units, Collections.singletonList(op));
  }

  public static String createTimedoutMessage(long beforeAwait,
                                             long duration,
                                             TimeUnit units,
                                             Collection<Operation> ops) {

    StringBuilder rv = new StringBuilder();
    Operation firstOp = ops.iterator().next();
    if (isBulkOperation(firstOp, ops)) {
      rv.append("bulk ");
    }
    if (firstOp.isPipeOperation()) {
      rv.append("pipe ");
    }

    long elapsed = convertUnit(System.nanoTime() - beforeAwait, units);

    rv.append(firstOp.getAPIType())
      .append(" operation timed out (").append(elapsed)
      .append(" >= ").append(duration)
      .append(" ").append(units).append(")");
    return createMessage(rv.toString(), ops);
  }

  /**
   * check bulk operation or not
   * @param op operation
   * @param ops number of operation (used in StoreOperationImpl, DeleteOperationImpl)
   */
  private static boolean isBulkOperation(Operation op, Collection<Operation> ops) {
    if (op instanceof StoreOperation || op instanceof DeleteOperation) {
      return ops.size() > 1;
    }
    return op.isBulkOperation();
  }

  public static String createMessage(String message,
                                     Collection<Operation> ops) {
    StringBuilder rv = new StringBuilder(message);
    rv.append(" - failing node");
    rv.append(ops.size() == 1 ? ": " : "s: ");
    boolean first = true;
    for (Operation op : ops) {
      if (first) {
        first = false;
      } else {
        rv.append(", ");
      }
      MemcachedNode node = op == null ? null : op.getHandlingNode();
      rv.append(node == null ? "<unknown>" : node.getNodeName());
      if (op != null) {
        rv.append(" [").append(op.getState()).append("]");
      }
      if (node != null) {
        rv.append(" [").append(node.getOpQueueStatus()).append("]");
        if (!node.isActive() && node.isFirstConnecting()) {
          rv.append(" (Not connected yet)");
        }
      }
    }
    return rv.toString();
  }

  private static long convertUnit(long nanos, TimeUnit unit) {
    return unit.convert(nanos, TimeUnit.NANOSECONDS);
  }

}
