/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceManagerConstants;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;

import com.google.common.annotations.VisibleForTesting;

public class NodeStatusUpdaterImpl extends AbstractService implements
    NodeStatusUpdater {

  public static final String YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS =
      YarnConfiguration.NM_PREFIX + "duration-to-track-stopped-containers";

  private static final Log LOG = LogFactory.getLog(NodeStatusUpdaterImpl.class);

  private final Object heartbeatMonitor = new Object();

  private final Context context;
  private final Dispatcher dispatcher;

  private NodeId nodeId;
  private long nextHeartBeatInterval;
  private ResourceTracker resourceTracker;
  private Resource totalResource;
  private int httpPort;
  private volatile boolean isStopped;
  private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private boolean tokenKeepAliveEnabled;
  private long tokenRemovalDelayMs;
  /** Keeps track of when the next keep alive request should be sent for an app*/
  private Map<ApplicationId, Long> appTokenKeepAliveMap =
      new HashMap<ApplicationId, Long>();
  private Random keepAliveDelayRandom = new Random();
  // It will be used to track recently stopped containers on node manager.
  private final Map<ContainerId, Long> recentlyStoppedContainers;
  // Duration for which to track recently stopped container.
  private long durationToTrackStoppedContainers;

  private final NodeHealthCheckerService healthChecker;
  private final NodeManagerMetrics metrics;

  private Runnable statusUpdaterRunnable;
  private Thread  statusUpdater;
  private long rmIdentifier = ResourceManagerConstants.RM_INVALID_IDENTIFIER;

  public NodeStatusUpdaterImpl(Context context, Dispatcher dispatcher,
      NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
    super(NodeStatusUpdaterImpl.class.getName());
    this.healthChecker = healthChecker;
    this.context = context;
    this.dispatcher = dispatcher;
    this.metrics = metrics;
    this.recentlyStoppedContainers =
        new LinkedHashMap<ContainerId, Long>();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    int memoryMb = 
        conf.getInt(
            YarnConfiguration.NM_PMEM_MB, YarnConfiguration.DEFAULT_NM_PMEM_MB);
    float vMemToPMem =             
        conf.getFloat(
            YarnConfiguration.NM_VMEM_PMEM_RATIO, 
            YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO); 
    int virtualMemoryMb = (int)Math.ceil(memoryMb * vMemToPMem);
    
    int virtualCores =
        conf.getInt(
            YarnConfiguration.NM_VCORES, YarnConfiguration.DEFAULT_NM_VCORES);

    this.totalResource = recordFactory.newRecordInstance(Resource.class);
    this.totalResource.setMemory(memoryMb);
    this.totalResource.setVirtualCores(virtualCores);
    metrics.addResource(totalResource);
    this.tokenKeepAliveEnabled = isTokenKeepAliveEnabled(conf);
    this.tokenRemovalDelayMs =
        conf.getInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    
    // Default duration to track stopped containers on nodemanager is 10Min.
    // This should not be assigned very large value as it will remember all the
    // containers stopped during that time.
    durationToTrackStoppedContainers =
        conf.getLong(YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS,
          600000);
    if (durationToTrackStoppedContainers < 0) {
      String message = "Invalid configuration for "
        + YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS + " default "
          + "value is 10Min(600000).";
      LOG.error(message);
      throw new YarnException(message);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS + " :"
        + durationToTrackStoppedContainers);
    }
    super.serviceInit(conf);
    LOG.info("Initialized nodemanager for " + nodeId + ":" +
        " physical-memory=" + memoryMb + " virtual-memory=" + virtualMemoryMb +
        " virtual-cores=" + virtualCores);
  }

  @Override
  protected void serviceStart() throws Exception {

    // NodeManager is the last service to start, so NodeId is available.
    this.nodeId = this.context.getNodeId();
    this.httpPort = this.context.getHttpPort();
    try {
      // Registration has to be in start so that ContainerManager can get the
      // perNM tokens needed to authenticate ContainerTokens.
      this.resourceTracker = getRMClient();
      registerWithRM();
      super.serviceStart();
      startStatusUpdater();
    } catch (Exception e) {
      String errorMessage = "Unexpected error starting NodeStatusUpdater";
      LOG.error(errorMessage, e);
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    // Interrupt the updater.
    this.isStopped = true;
    stopRMProxy();
    super.serviceStop();
  }

  protected void rebootNodeStatusUpdater() {
    // Interrupt the updater.
    this.isStopped = true;

    try {
      statusUpdater.join();
      registerWithRM();
      statusUpdater = new Thread(statusUpdaterRunnable, "Node Status Updater");
      this.isStopped = false;
      statusUpdater.start();
      LOG.info("NodeStatusUpdater thread is reRegistered and restarted");
    } catch (Exception e) {
      String errorMessage = "Unexpected error rebooting NodeStatusUpdater";
      LOG.error(errorMessage, e);
      throw new YarnRuntimeException(e);
    }
  }

  @VisibleForTesting
  protected void stopRMProxy() {
    if(this.resourceTracker != null) {
      RPC.stopProxy(this.resourceTracker);
    }
  }

  @Private
  protected boolean isTokenKeepAliveEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)
        && UserGroupInformation.isSecurityEnabled();
  }

  @VisibleForTesting
  protected ResourceTracker getRMClient() throws IOException {
    Configuration conf = getConfig();
    return ServerRMProxy.createRMProxy(conf, ResourceTracker.class);
  }

  @VisibleForTesting
  protected void registerWithRM() throws YarnException, IOException {
    RegisterNodeManagerRequest request =
        recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    request.setHttpPort(this.httpPort);
    request.setResource(this.totalResource);
    request.setNodeId(this.nodeId);
    RegisterNodeManagerResponse regNMResponse =
        resourceTracker.registerNodeManager(request);
    this.rmIdentifier = regNMResponse.getRMIdentifier();
    // if the Resourcemanager instructs NM to shutdown.
    if (NodeAction.SHUTDOWN.equals(regNMResponse.getNodeAction())) {
      String message =
          "Message from ResourceManager: "
              + regNMResponse.getDiagnosticsMessage();
      throw new YarnRuntimeException(
        "Recieved SHUTDOWN signal from Resourcemanager ,Registration of NodeManager failed, "
            + message);
    }

    MasterKey masterKey = regNMResponse.getContainerTokenMasterKey();
    // do this now so that its set before we start heartbeating to RM
    // It is expected that status updater is started by this point and
    // RM gives the shared secret in registration during
    // StatusUpdater#start().
    if (masterKey != null) {
      this.context.getContainerTokenSecretManager().setMasterKey(masterKey);
    }
    
    masterKey = regNMResponse.getNMTokenMasterKey();
    if (masterKey != null) {
      this.context.getNMTokenSecretManager().setMasterKey(masterKey);
    }

    LOG.info("Registered with ResourceManager as " + this.nodeId
        + " with total resource of " + this.totalResource);
    LOG.info("Notifying ContainerManager to unblock new container-requests");
    ((ContainerManagerImpl) this.context.getContainerManager())
      .setBlockNewContainerRequests(false);
  }

  private List<ApplicationId> createKeepAliveApplicationList() {
    if (!tokenKeepAliveEnabled) {
      return Collections.emptyList();
    }

    List<ApplicationId> appList = new ArrayList<ApplicationId>();
    for (Iterator<Entry<ApplicationId, Long>> i =
        this.appTokenKeepAliveMap.entrySet().iterator(); i.hasNext();) {
      Entry<ApplicationId, Long> e = i.next();
      ApplicationId appId = e.getKey();
      Long nextKeepAlive = e.getValue();
      if (!this.context.getApplications().containsKey(appId)) {
        // Remove if the application has finished.
        i.remove();
      } else if (System.currentTimeMillis() > nextKeepAlive) {
        // KeepAlive list for the next hearbeat.
        appList.add(appId);
        trackAppForKeepAlive(appId);
      }
    }
    return appList;
  }

  @Override
  public NodeStatus getNodeStatusAndUpdateContainersInContext() {

    NodeStatus nodeStatus = recordFactory.newRecordInstance(NodeStatus.class);
    nodeStatus.setNodeId(this.nodeId);

    int numActiveContainers = 0;
    List<ContainerStatus> containersStatuses = new ArrayList<ContainerStatus>();
    for (Iterator<Entry<ContainerId, Container>> i =
        this.context.getContainers().entrySet().iterator(); i.hasNext();) {
      Entry<ContainerId, Container> e = i.next();
      ContainerId containerId = e.getKey();
      Container container = e.getValue();

      // Clone the container to send it to the RM
      org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus = 
          container.cloneAndGetContainerStatus();
      containersStatuses.add(containerStatus);
      ++numActiveContainers;
      LOG.info("Sending out status for container: " + containerStatus);

      if (containerStatus.getState() == ContainerState.COMPLETE) {
        // Remove
        i.remove();
        // Adding to finished containers cache. Cache will keep it around at
        // least for #durationToTrackStoppedContainers duration. In the
        // subsequent call to stop container it will get removed from cache.
        addStoppedContainersToCache(containerId);
        
        LOG.info("Removed completed container " + containerId);
      }
    }
    nodeStatus.setContainersStatuses(containersStatuses);

    LOG.debug(this.nodeId + " sending out status for "
        + numActiveContainers + " containers");

    NodeHealthStatus nodeHealthStatus = this.context.getNodeHealthStatus();
    nodeHealthStatus.setHealthReport(healthChecker.getHealthReport());
    nodeHealthStatus.setIsNodeHealthy(healthChecker.isHealthy());
    nodeHealthStatus.setLastHealthReportTime(
        healthChecker.getLastHealthReportTime());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node's health-status : " + nodeHealthStatus.getIsNodeHealthy()
                + ", " + nodeHealthStatus.getHealthReport());
    }
    nodeStatus.setNodeHealthStatus(nodeHealthStatus);

    List<ApplicationId> keepAliveAppIds = createKeepAliveApplicationList();
    nodeStatus.setKeepAliveApplications(keepAliveAppIds);
    
    return nodeStatus;
  }

  private void trackAppsForKeepAlive(List<ApplicationId> appIds) {
    if (tokenKeepAliveEnabled && appIds != null && appIds.size() > 0) {
      for (ApplicationId appId : appIds) {
        trackAppForKeepAlive(appId);
      }
    }
  }

  private void trackAppForKeepAlive(ApplicationId appId) {
    // Next keepAlive request for app between 0.7 & 0.9 of when the token will
    // likely expire.
    long nextTime = System.currentTimeMillis()
    + (long) (0.7 * tokenRemovalDelayMs + (0.2 * tokenRemovalDelayMs
        * keepAliveDelayRandom.nextInt(100))/100);
    appTokenKeepAliveMap.put(appId, nextTime);
  }

  @Override
  public void sendOutofBandHeartBeat() {
    synchronized (this.heartbeatMonitor) {
      this.heartbeatMonitor.notify();
    }
  }

  public boolean isContainerRecentlyStopped(ContainerId containerId) {
    synchronized (recentlyStoppedContainers) {
      return recentlyStoppedContainers.containsKey(containerId);
    }
  }
  
  @Private
  @VisibleForTesting
  public void addStoppedContainersToCache(ContainerId containerId) {
    synchronized (recentlyStoppedContainers) {
      removeVeryOldStoppedContainersFromCache();
      recentlyStoppedContainers.put(containerId,
        System.currentTimeMillis() + durationToTrackStoppedContainers);
    }
  }
  
  @Override
  public void clearFinishedContainersFromCache() {
    synchronized (recentlyStoppedContainers) {
      recentlyStoppedContainers.clear();
    }
  }
  
  @Private
  @VisibleForTesting
  public void removeVeryOldStoppedContainersFromCache() {
    synchronized (recentlyStoppedContainers) {
      long currentTime = System.currentTimeMillis();
      Iterator<ContainerId> i =
          recentlyStoppedContainers.keySet().iterator();
      while (i.hasNext()) {
        if (recentlyStoppedContainers.get(i.next()) < currentTime) {
          i.remove();
        } else {
          break;
        }
      }
    }
  }
  
  @Override
  public long getRMIdentifier() {
    return this.rmIdentifier;
  }

  protected void startStatusUpdater() {

    statusUpdaterRunnable = new Runnable() {
      @Override
      @SuppressWarnings("unchecked")
      public void run() {
        int lastHeartBeatID = 0;
        while (!isStopped) {
          // Send heartbeat
          try {
            NodeHeartbeatResponse response = null;
            NodeStatus nodeStatus = getNodeStatusAndUpdateContainersInContext();
            nodeStatus.setResponseId(lastHeartBeatID);
            
            NodeHeartbeatRequest request = recordFactory
                .newRecordInstance(NodeHeartbeatRequest.class);
            request.setNodeStatus(nodeStatus);
            request
              .setLastKnownContainerTokenMasterKey(NodeStatusUpdaterImpl.this.context
                .getContainerTokenSecretManager().getCurrentKey());
            request
              .setLastKnownNMTokenMasterKey(NodeStatusUpdaterImpl.this.context
                .getNMTokenSecretManager().getCurrentKey());
            response = resourceTracker.nodeHeartbeat(request);
            //get next heartbeat interval from response
            nextHeartBeatInterval = response.getNextHeartBeatInterval();
            updateMasterKeys(response);

            if (response.getNodeAction() == NodeAction.SHUTDOWN) {
              LOG
                .warn("Recieved SHUTDOWN signal from Resourcemanager as part of heartbeat,"
                    + " hence shutting down.");
              LOG.warn("Message from ResourceManager: "
                  + response.getDiagnosticsMessage());
              dispatcher.getEventHandler().handle(
                  new NodeManagerEvent(NodeManagerEventType.SHUTDOWN));
              break;
            }
            if (response.getNodeAction() == NodeAction.RESYNC) {
              LOG.warn("Node is out of sync with ResourceManager,"
                  + " hence resyncing.");
              LOG.warn("Message from ResourceManager: "
                  + response.getDiagnosticsMessage());
              // Invalidate the RMIdentifier while resync
              NodeStatusUpdaterImpl.this.rmIdentifier =
                  ResourceManagerConstants.RM_INVALID_IDENTIFIER;
              dispatcher.getEventHandler().handle(
                  new NodeManagerEvent(NodeManagerEventType.RESYNC));
              break;
            }

            lastHeartBeatID = response.getResponseId();
            List<ContainerId> containersToCleanup = response
                .getContainersToCleanup();
            if (containersToCleanup.size() != 0) {
              dispatcher.getEventHandler().handle(
                  new CMgrCompletedContainersEvent(containersToCleanup, 
                      CMgrCompletedContainersEvent.Reason.BY_RESOURCEMANAGER));
            }
            List<ApplicationId> appsToCleanup =
                response.getApplicationsToCleanup();
            //Only start tracking for keepAlive on FINISH_APP
            trackAppsForKeepAlive(appsToCleanup);
            if (appsToCleanup.size() != 0) {
              dispatcher.getEventHandler().handle(
                  new CMgrCompletedAppsEvent(appsToCleanup));
            }
          } catch (ConnectException e) {
            //catch and throw the exception if tried MAX wait time to connect RM
            dispatcher.getEventHandler().handle(
                new NodeManagerEvent(NodeManagerEventType.SHUTDOWN));
            throw new YarnRuntimeException(e);
          } catch (Throwable e) {

            // TODO Better error handling. Thread can die with the rest of the
            // NM still running.
            LOG.error("Caught exception in status-updater", e);
          } finally {
            synchronized (heartbeatMonitor) {
              nextHeartBeatInterval = nextHeartBeatInterval <= 0 ?
                  YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS :
                    nextHeartBeatInterval;
              try {
                heartbeatMonitor.wait(nextHeartBeatInterval);
              } catch (InterruptedException e) {
                // Do Nothing
              }
            }
          }
        }
      }

      private void updateMasterKeys(NodeHeartbeatResponse response) {
        // See if the master-key has rolled over
        MasterKey updatedMasterKey = response.getContainerTokenMasterKey();
        if (updatedMasterKey != null) {
          // Will be non-null only on roll-over on RM side
          context.getContainerTokenSecretManager().setMasterKey(updatedMasterKey);
        }
        
        updatedMasterKey = response.getNMTokenMasterKey();
        if (updatedMasterKey != null) {
          context.getNMTokenSecretManager().setMasterKey(updatedMasterKey);
        }
      }
    };
    statusUpdater =
        new Thread(statusUpdaterRunnable, "Node Status Updater");
    statusUpdater.start();
  }
  
  
}
