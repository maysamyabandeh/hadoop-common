package org.apache.hadoop.yarn.client.api.impl;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;


class CapacitySchedulerForUniqueNM extends CapacityScheduler {

  /**
   * The nodes are identified by their host name only and it makes it difficult
   * to test with multiple nodes in a single machine. We attach the port to make
   * the host name unique.
   * @param nodeId
   * @return
   */
  public static String getUniqueHostNameForTest(NodeId nodeId) {
    return nodeId.getHost() + nodeId.getPort();
  }

  @Override
  protected FiCaSchedulerNode newSchedulerNode(RMNode rmNode) {
    return new FiCaSchedulerNodeForUniqueNM(rmNode); 
  }
  
  static class FiCaSchedulerNodeForUniqueNM extends FiCaSchedulerNode {
    public FiCaSchedulerNodeForUniqueNM(RMNode node) {
      super(node);
    }

    @Override
    public String getHostName() {
      return getUniqueHostNameForTest(this.getRMNode().getNodeID());
    }
  }
}