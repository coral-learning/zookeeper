## Zookeeper之服务端ZookeeperServer源码分析
### 1.概览
```
                                    ZooKeeperServer
                                             ↑
                                    QuorumZooKeeperServer
                           ↗                 ↑                      ↖
            LearnerZooKeeperServer ReadOnlyZooKeeperServer LeaderZooKeeperServer
                  ↗             ↖
ObserverZooKeeperServer FollowerZooKeeperServer
```

### 2.ZooKeeperServer
#### 2.1 状态

ZooKeeperServer分别为：INITIAL/RUNNING/SHUTDOWN/ERROR
```
protected enum State {
    INITIAL,
    RUNNING,
    SHUTDOWN,
    ERROR
}
```
#### 2.2 核心成员

```
protected ZooKeeperServerBean jmxServerBean;

protected DataTreeBean jmxDataTreeBean;

public static final int DEFAULT_TICK_TIME = 3000;
protected int tickTime = DEFAULT_TICK_TIME;
/** value of -1 indicates unset, use default */
protected int minSessionTimeout = -1;
/** value of -1 indicates unset, use default */
protected int maxSessionTimeout = -1;
/** Socket listen backlog. Value of -1 indicates unset */
protected int listenBacklog = -1;
protected SessionTracker sessionTracker;
private FileTxnSnapLog txnLogFactory = null;
private ZKDatabase zkDb;
private ResponseCache readResponseCache;
private ResponseCache getChildrenResponseCache;
private final AtomicLong hzxid = new AtomicLong(0);
public static final Exception ok = new Exception("No prob");
protected RequestProcessor firstProcessor;
protected JvmPauseMonitor jvmPauseMonitor;
protected volatile State state = State.INITIAL;
private boolean isResponseCachingEnabled = true;
/* contains the configuration file content read at startup */
protected String initialConfig;
private final RequestPathMetricsCollector requestPathMetricsCollector;

private boolean localSessionEnabled = false;
```

#### 2.3 核心方法
```

/**
 *  Restore sessions and data
 */
public void loadData()

public synchronized void startup() {
    if (sessionTracker == null) {
        createSessionTracker();
    }
    startSessionTracker();
    setupRequestProcessors();

    startRequestThrottler();

    registerJMX();

    startJvmPauseMonitor();

    registerMetrics();

    setState(State.RUNNING);

    requestPathMetricsCollector.start();

    localSessionEnabled = sessionTracker.isLocalSessionsEnabled();
    notifyAll();
}


/**
 * Shut down the server instance
 * @param fullyShutDown true if another server using the same database will not replace this one in the same process
 */
public synchronized void shutdown(boolean fullyShutDown) {
    if (!canShutdown()) {
        LOG.debug("ZooKeeper server is not running, so not proceeding to shutdown!");
        return;
    }
    LOG.info("shutting down");

    // new RuntimeException("Calling shutdown").printStackTrace();
    setState(State.SHUTDOWN);

    // unregister all metrics that are keeping a strong reference to this object
    // subclasses will do their specific clean up
    unregisterMetrics();

    if (requestThrottler != null) {
        requestThrottler.shutdown();
    }

    // Since sessionTracker and syncThreads poll we just have to
    // set running to false and they will detect it during the poll
    // interval.
    if (sessionTracker != null) {
        sessionTracker.shutdown();
    }
    if (firstProcessor != null) {
        firstProcessor.shutdown();
    }
    if (jvmPauseMonitor != null) {
        jvmPauseMonitor.serviceStop();
    }

    if (zkDb != null) {
        if (fullyShutDown) {
            zkDb.clear();
        } else {
            // else there is no need to clear the database
            //  * When a new quorum is established we can still apply the diff
            //    on top of the same zkDb data
            //  * If we fetch a new snapshot from leader, the zkDb will be
            //    cleared anyway before loading the snapshot
            try {
                //This will fast forward the database to the latest recorded transactions
                zkDb.fastForwardDataBase();
            } catch (IOException e) {
                LOG.error("Error updating DB", e);
                zkDb.clear();
            }
        }
    }

    requestPathMetricsCollector.shutdown();
    unregisterJMX();
}

```


    

### 3.QuorumZooKeeperServer
* public Request checkUpgradeSession(Request request)
* public void upgrade(long sessionId)
* public void dumpConf(PrintWriter pwriter) 
* public void dumpMonitorValues(BiConsumer<String, Object> response) 

```
public final QuorumPeer self;
protected UpgradeableSessionTracker upgradeableSessionTracker;

protected QuorumZooKeeperServer(FileTxnSnapLog logFactory, int tickTime, int minSessionTimeout,
                                int maxSessionTimeout, int listenBacklog, ZKDatabase zkDb, QuorumPeer self) {
    super(logFactory, tickTime, minSessionTimeout, maxSessionTimeout, listenBacklog, zkDb, self.getInitialConfig());
    this.self = self;
}

@Override
protected void startSessionTracker() {
    upgradeableSessionTracker = (UpgradeableSessionTracker) sessionTracker;
    upgradeableSessionTracker.start();
}

public Request checkUpgradeSession(Request request) throws IOException, KeeperException {
    // If this is a request for a local session and it is to
    // create an ephemeral node, then upgrade the session and return
    // a new session request for the leader.
    // This is called by the request processor thread (either follower
    // or observer request processor), which is unique to a learner.
    // So will not be called concurrently by two threads.
    if ((request.type != OpCode.create && request.type != OpCode.create2 && request.type != OpCode.multi)
        || !upgradeableSessionTracker.isLocalSession(request.sessionId)) {
        return null;
    }

    if (OpCode.multi == request.type) {
        MultiOperationRecord multiTransactionRecord = new MultiOperationRecord();
        request.request.rewind();
        ByteBufferInputStream.byteBuffer2Record(request.request, multiTransactionRecord);
        request.request.rewind();
        boolean containsEphemeralCreate = false;
        for (Op op : multiTransactionRecord) {
            if (op.getType() == OpCode.create || op.getType() == OpCode.create2) {
                CreateRequest createRequest = (CreateRequest) op.toRequestRecord();
                CreateMode createMode = CreateMode.fromFlag(createRequest.getFlags());
                if (createMode.isEphemeral()) {
                    containsEphemeralCreate = true;
                    break;
                }
            }
        }
        if (!containsEphemeralCreate) {
            return null;
        }
    } else {
        CreateRequest createRequest = new CreateRequest();
        request.request.rewind();
        ByteBufferInputStream.byteBuffer2Record(request.request, createRequest);
        request.request.rewind();
        CreateMode createMode = CreateMode.fromFlag(createRequest.getFlags());
        if (!createMode.isEphemeral()) {
            return null;
        }
    }

    // Uh oh.  We need to upgrade before we can proceed.
    if (!self.isLocalSessionsUpgradingEnabled()) {
        throw new KeeperException.EphemeralOnLocalSessionException();
    }

    return makeUpgradeRequest(request.sessionId);
}

private Request makeUpgradeRequest(long sessionId) {
    // Make sure to atomically check local session status, upgrade
    // session, and make the session creation request.  This is to
    // avoid another thread upgrading the session in parallel.
    synchronized (upgradeableSessionTracker) {
        if (upgradeableSessionTracker.isLocalSession(sessionId)) {
            int timeout = upgradeableSessionTracker.upgradeSession(sessionId);
            ByteBuffer to = ByteBuffer.allocate(4);
            to.putInt(timeout);
            return new Request(null, sessionId, 0, OpCode.createSession, to, null);
        }
    }
    return null;
}

/**
 * Implements the SessionUpgrader interface,
 *
 * @param sessionId
 */
public void upgrade(long sessionId) {
    Request request = makeUpgradeRequest(sessionId);
    if (request != null) {
        LOG.info("Upgrading session 0x{}", Long.toHexString(sessionId));
        // This must be a global request
        submitRequest(request);
    }
}

@Override
protected void setLocalSessionFlag(Request si) {
    // We need to set isLocalSession to tree for these type of request
    // so that the request processor can process them correctly.
    switch (si.type) {
    case OpCode.createSession:
        if (self.areLocalSessionsEnabled()) {
            // All new sessions local by default.
            si.setLocalSession(true);
        }
        break;
    case OpCode.closeSession:
        String reqType = "global";
        if (upgradeableSessionTracker.isLocalSession(si.sessionId)) {
            si.setLocalSession(true);
            reqType = "local";
        }
        LOG.info("Submitting {} closeSession request for session 0x{}", reqType, Long.toHexString(si.sessionId));
        break;
    default:
        break;
    }
}

@Override
public void dumpConf(PrintWriter pwriter) {
    super.dumpConf(pwriter);
    ...
}

@Override
protected void setState(State state) {
    this.state = state;
}

@Override
protected void registerMetrics() {
    ...
}

@Override
protected void unregisterMetrics() {
    ...
}

@Override
public void dumpMonitorValues(BiConsumer<String, Object> response) {
    super.dumpMonitorValues(response);
    response.accept("peer_state", self.getDetailedPeerState());
}
```
### ReadOnlyZooKeeperServer

### LeaderZooKeeperServer
```
public class LeaderZooKeeperServer extends QuorumZooKeeperServer {

    private ContainerManager containerManager;  // guarded by sync

    CommitProcessor commitProcessor;

    PrepRequestProcessor prepRequestProcessor;


    @Override
    protected void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        RequestProcessor toBeAppliedProcessor = new Leader.ToBeAppliedRequestProcessor(finalProcessor, getLeader());
        commitProcessor = new CommitProcessor(toBeAppliedProcessor, Long.toString(getServerId()), false, getZooKeeperServerListener());
        commitProcessor.start();
        ProposalRequestProcessor proposalProcessor = new ProposalRequestProcessor(this, commitProcessor);
        proposalProcessor.initialize();
        prepRequestProcessor = new PrepRequestProcessor(this, proposalProcessor);
        prepRequestProcessor.start();
        firstProcessor = new LeaderRequestProcessor(this, prepRequestProcessor);

        setupContainerManager();
    }

    private synchronized void setupContainerManager() {
        containerManager = new ContainerManager(
            getZKDatabase(),
            prepRequestProcessor,
            Integer.getInteger("znode.container.checkIntervalMs", (int) TimeUnit.MINUTES.toMillis(1)),
            Integer.getInteger("znode.container.maxPerMinute", 10000),
            Long.getLong("znode.container.maxNeverUsedIntervalMs", 0)
        );
    }

    @Override
    public synchronized void startup() {
        super.startup();
        if (containerManager != null) {
            containerManager.start();
        }
    }

    @Override
    protected void registerMetrics() {
        super.registerMetrics();

        MetricsContext rootContext = ServerMetrics.getMetrics().getMetricsProvider().getRootContext();

        rootContext.registerGauge("learners", () -> {
            return getLeader().getLearners().size();
        });
        rootContext.registerGauge("synced_followers", () -> {
            return getLeader().getForwardingFollowers().size();
        });
        rootContext.registerGauge("synced_non_voting_followers", () -> {
            return getLeader().getNonVotingFollowers().size();
        });

        rootContext.registerGauge("synced_observers", self::getSynced_observers_metric);

        rootContext.registerGauge("pending_syncs", () -> {
            return getLeader().getNumPendingSyncs();
        });
        rootContext.registerGauge("leader_uptime", () -> {
            return getLeader().getUptime();
        });
        rootContext.registerGauge("last_proposal_size", () -> {
            return getLeader().getProposalStats().getLastBufferSize();
        });
        rootContext.registerGauge("max_proposal_size", () -> {
            return getLeader().getProposalStats().getMaxBufferSize();
        });
        rootContext.registerGauge("min_proposal_size", () -> {
            return getLeader().getProposalStats().getMinBufferSize();
        });
    }

    @Override
    protected void unregisterMetrics() {
        super.unregisterMetrics();

        MetricsContext rootContext = ServerMetrics.getMetrics().getMetricsProvider().getRootContext();
        rootContext.unregisterGauge("learners");
        rootContext.unregisterGauge("synced_followers");
        rootContext.unregisterGauge("synced_non_voting_followers");
        rootContext.unregisterGauge("synced_observers");
        rootContext.unregisterGauge("pending_syncs");
        rootContext.unregisterGauge("leader_uptime");

        rootContext.unregisterGauge("last_proposal_size");
        rootContext.unregisterGauge("max_proposal_size");
        rootContext.unregisterGauge("min_proposal_size");
    }

    @Override
    public synchronized void shutdown() {
        if (containerManager != null) {
            containerManager.stop();
        }
        super.shutdown();
    }

    @Override
    public int getGlobalOutstandingLimit() {
        int divisor = self.getQuorumSize() > 2 ? self.getQuorumSize() - 1 : 1;
        int globalOutstandingLimit = super.getGlobalOutstandingLimit() / divisor;
        return globalOutstandingLimit;
    }

    @Override
    public void createSessionTracker() {
        sessionTracker = new LeaderSessionTracker(
            this,
            getZKDatabase().getSessionWithTimeOuts(),
            tickTime,
            self.getId(),
            self.areLocalSessionsEnabled(),
            getZooKeeperServerListener());
    }

    public boolean touch(long sess, int to) {
        return sessionTracker.touchSession(sess, to);
    }

    public boolean checkIfValidGlobalSession(long sess, int to) {
        if (self.areLocalSessionsEnabled() && !upgradeableSessionTracker.isGlobalSession(sess)) {
            return false;
        }
        return sessionTracker.touchSession(sess, to);
    }

    /**
     * Requests coming from the learner should go directly to
     * PrepRequestProcessor
     *
     * @param request
     */
    public void submitLearnerRequest(Request request) {
        /*
         * Requests coming from the learner should have gone through
         * submitRequest() on each server which already perform some request
         * validation, so we don't need to do it again.
         *
         * Additionally, LearnerHandler should start submitting requests into
         * the leader's pipeline only when the leader's server is started, so we
         * can submit the request directly into PrepRequestProcessor.
         *
         * This is done so that requests from learners won't go through
         * LeaderRequestProcessor which perform local session upgrade.
         */
        prepRequestProcessor.processRequest(request);
    }

    @Override
    protected void registerJMX() {
        ...
    }

    public void registerJMX(LeaderBean leaderBean, LocalPeerBean localPeerBean) {
        ...
    }

    boolean registerJMX(LearnerHandlerBean handlerBean) {
        ....
    }

    @Override
    protected void unregisterJMX() {
       ....
    }

    protected void unregisterJMX(Leader leader) {
        ....
    }

    @Override
    public String getState() {
        return "leader";
    }

    /**
     * Returns the id of the associated QuorumPeer, which will do for a unique
     * id of this server.
     */
    @Override
    public long getServerId() {
        return self.getId();
    }

    @Override
    protected void revalidateSession(ServerCnxn cnxn, long sessionId, int sessionTimeout) throws IOException {
        super.revalidateSession(cnxn, sessionId, sessionTimeout);
        try {
            // setowner as the leader itself, unless updated
            // via the follower handlers
            setOwner(sessionId, ServerCnxn.me);
        } catch (SessionExpiredException e) {
            // this is ok, it just means that the session revalidation failed.
        }
    }

}

```

### LearnerZooKeeperServer

```
public abstract class LearnerZooKeeperServer extends QuorumZooKeeperServer {

    /*
     * Request processors
     */
    protected CommitProcessor commitProcessor;
    protected SyncRequestProcessor syncProcessor;

    public LearnerZooKeeperServer(FileTxnSnapLog logFactory, int tickTime, int minSessionTimeout, int maxSessionTimeout, int listenBacklog, ZKDatabase zkDb, QuorumPeer self) throws IOException {
        super(logFactory, tickTime, minSessionTimeout, maxSessionTimeout, listenBacklog, zkDb, self);
    }

    /**
     * Abstract method to return the learner associated with this server.
     * Since the Learner may change under our feet (when QuorumPeer reassigns
     * it) we can't simply take a reference here. Instead, we need the
     * subclasses to implement this.
     */
    public abstract Learner getLearner();

    /**
     * Returns the current state of the session tracker. This is only currently
     * used by a Learner to build a ping response packet.
     *
     */
    protected Map<Long, Integer> getTouchSnapshot() {
        if (sessionTracker != null) {
            return ((LearnerSessionTracker) sessionTracker).snapshot();
        }
        Map<Long, Integer> map = Collections.emptyMap();
        return map;
    }

    /**
     * Returns the id of the associated QuorumPeer, which will do for a unique
     * id of this server.
     */
    @Override
    public long getServerId() {
        return self.getId();
    }

    @Override
    public void createSessionTracker() {
        sessionTracker = new LearnerSessionTracker(
            this,
            getZKDatabase().getSessionWithTimeOuts(),
            this.tickTime,
            self.getId(),
            self.areLocalSessionsEnabled(),
            getZooKeeperServerListener());
    }

    @Override
    protected void revalidateSession(ServerCnxn cnxn, long sessionId, int sessionTimeout) throws IOException {
        if (upgradeableSessionTracker.isLocalSession(sessionId)) {
            super.revalidateSession(cnxn, sessionId, sessionTimeout);
        } else {
            getLearner().validateSession(cnxn, sessionId, sessionTimeout);
        }
    }

    @Override
    protected void registerJMX() {
        // register with JMX
        try {
            jmxDataTreeBean = new DataTreeBean(getZKDatabase().getDataTree());
            MBeanRegistry.getInstance().register(jmxDataTreeBean, jmxServerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxDataTreeBean = null;
        }
    }

    public void registerJMX(ZooKeeperServerBean serverBean, LocalPeerBean localPeerBean) {
        // register with JMX
        if (self.jmxLeaderElectionBean != null) {
            try {
                MBeanRegistry.getInstance().unregister(self.jmxLeaderElectionBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
        }

        try {
            jmxServerBean = serverBean;
            MBeanRegistry.getInstance().register(serverBean, localPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxServerBean = null;
        }
    }

    @Override
    protected void unregisterJMX() {
        // unregister from JMX
        try {
            if (jmxDataTreeBean != null) {
                MBeanRegistry.getInstance().unregister(jmxDataTreeBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxDataTreeBean = null;
    }

    protected void unregisterJMX(Learner peer) {
        // unregister from JMX
        try {
            if (jmxServerBean != null) {
                MBeanRegistry.getInstance().unregister(jmxServerBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxServerBean = null;
    }

    @Override
    public synchronized void shutdown() {
        if (!canShutdown()) {
            LOG.debug("ZooKeeper server is not running, so not proceeding to shutdown!");
            return;
        }
        LOG.info("Shutting down");
        try {
            super.shutdown();
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }
        try {
            if (syncProcessor != null) {
                syncProcessor.shutdown();
            }
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception in syncprocessor shutdown", e);
        }
    }

}


```

### ObserverZooKeeperServer

```
/**
 * A ZooKeeperServer for the Observer node type. Not much is different, but
 * we anticipate specializing the request processors in the future.
 *
 */
public class ObserverZooKeeperServer extends LearnerZooKeeperServer {

    private static final Logger LOG = LoggerFactory.getLogger(ObserverZooKeeperServer.class);

    /**
     * Enable since request processor for writing txnlog to disk and
     * take periodic snapshot. Default is ON.
     */

    private boolean syncRequestProcessorEnabled = this.self.getSyncEnabled();

    /*
     * Pending sync requests
     */ ConcurrentLinkedQueue<Request> pendingSyncs = new ConcurrentLinkedQueue<Request>();

    ObserverZooKeeperServer(FileTxnSnapLog logFactory, QuorumPeer self, ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout, self.maxSessionTimeout, self.clientPortListenBacklog, zkDb, self);
        LOG.info("syncEnabled ={}", syncRequestProcessorEnabled);
    }

    public Observer getObserver() {
        return self.observer;
    }

    @Override
    public Learner getLearner() {
        return self.observer;
    }

    /**
     * Unlike a Follower, which sees a full request only during the PROPOSAL
     * phase, Observers get all the data required with the INFORM packet.
     * This method commits a request that has been unpacked by from an INFORM
     * received from the Leader.
     *
     * @param request
     */
    public void commitRequest(Request request) {
        if (syncRequestProcessorEnabled) {
            // Write to txnlog and take periodic snapshot
            syncProcessor.processRequest(request);
        }
        commitProcessor.commit(request);
    }

    /**
     * Set up the request processors for an Observer:
     * firstProcesor-&gt;commitProcessor-&gt;finalProcessor
     */
    @Override
    protected void setupRequestProcessors() {
        // We might consider changing the processor behaviour of
        // Observers to, for example, remove the disk sync requirements.
        // Currently, they behave almost exactly the same as followers.
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        commitProcessor = new CommitProcessor(finalProcessor, Long.toString(getServerId()), true, getZooKeeperServerListener());
        commitProcessor.start();
        firstProcessor = new ObserverRequestProcessor(this, commitProcessor);
        ((ObserverRequestProcessor) firstProcessor).start();

        /*
         * Observer should write to disk, so that the it won't request
         * too old txn from the leader which may lead to getting an entire
         * snapshot.
         *
         * However, this may degrade performance as it has to write to disk
         * and do periodic snapshot which may double the memory requirements
         */
        if (syncRequestProcessorEnabled) {
            syncProcessor = new SyncRequestProcessor(this, null);
            syncProcessor.start();
        }
    }

    /*
     * Process a sync request
     */
    public synchronized void sync() {
        if (pendingSyncs.size() == 0) {
            LOG.warn("Not expecting a sync.");
            return;
        }

        Request r = pendingSyncs.remove();
        commitProcessor.commit(r);
    }

    @Override
    public String getState() {
        return "observer";
    }

    @Override
    public synchronized void shutdown() {
        if (!canShutdown()) {
            LOG.debug("ZooKeeper server is not running, so not proceeding to shutdown!");
            return;
        }
        super.shutdown();
        if (syncRequestProcessorEnabled && syncProcessor != null) {
            syncProcessor.shutdown();
        }
    }

    @Override
    public void dumpMonitorValues(BiConsumer<String, Object> response) {
        super.dumpMonitorValues(response);
        response.accept("observer_master_id", getObserver().getLearnerMasterId());
    }

}

```

### FollowerZooKeeperServer

```
/**
 * Just like the standard ZooKeeperServer. We just replace the request
 * processors: FollowerRequestProcessor -&gt; CommitProcessor -&gt;
 * FinalRequestProcessor
 *
 * A SyncRequestProcessor is also spawned off to log proposals from the leader.
 */
public class FollowerZooKeeperServer extends LearnerZooKeeperServer {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerZooKeeperServer.class);

    /*
     * Pending sync requests
     */ ConcurrentLinkedQueue<Request> pendingSyncs;

    /**
     * @throws IOException
     */
    FollowerZooKeeperServer(FileTxnSnapLog logFactory, QuorumPeer self, ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout, self.maxSessionTimeout, self.clientPortListenBacklog, zkDb, self);
        this.pendingSyncs = new ConcurrentLinkedQueue<Request>();
    }

    public Follower getFollower() {
        return self.follower;
    }

    @Override
    protected void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        commitProcessor = new CommitProcessor(finalProcessor, Long.toString(getServerId()), true, getZooKeeperServerListener());
        commitProcessor.start();
        firstProcessor = new FollowerRequestProcessor(this, commitProcessor);
        ((FollowerRequestProcessor) firstProcessor).start();
        syncProcessor = new SyncRequestProcessor(this, new SendAckRequestProcessor(getFollower()));
        syncProcessor.start();
    }

    LinkedBlockingQueue<Request> pendingTxns = new LinkedBlockingQueue<Request>();

    public void logRequest(TxnHeader hdr, Record txn, TxnDigest digest) {
        Request request = new Request(hdr.getClientId(), hdr.getCxid(), hdr.getType(), hdr, txn, hdr.getZxid());
        request.setTxnDigest(digest);
        if ((request.zxid & 0xffffffffL) != 0) {
            pendingTxns.add(request);
        }
        syncProcessor.processRequest(request);
    }

    /**
     * When a COMMIT message is received, eventually this method is called,
     * which matches up the zxid from the COMMIT with (hopefully) the head of
     * the pendingTxns queue and hands it to the commitProcessor to commit.
     * @param zxid - must correspond to the head of pendingTxns if it exists
     */
    public void commit(long zxid) {
        if (pendingTxns.size() == 0) {
            LOG.warn("Committing " + Long.toHexString(zxid) + " without seeing txn");
            return;
        }
        long firstElementZxid = pendingTxns.element().zxid;
        if (firstElementZxid != zxid) {
            LOG.error("Committing zxid 0x" + Long.toHexString(zxid)
                      + " but next pending txn 0x" + Long.toHexString(firstElementZxid));
            ServiceUtils.requestSystemExit(ExitCode.UNMATCHED_TXN_COMMIT.getValue());
        }
        Request request = pendingTxns.remove();
        request.logLatency(ServerMetrics.getMetrics().COMMIT_PROPAGATION_LATENCY);
        commitProcessor.commit(request);
    }

    public synchronized void sync() {
        if (pendingSyncs.size() == 0) {
            LOG.warn("Not expecting a sync.");
            return;
        }

        Request r = pendingSyncs.remove();
        if (r instanceof LearnerSyncRequest) {
            LearnerSyncRequest lsr = (LearnerSyncRequest) r;
            lsr.fh.queuePacket(new QuorumPacket(Leader.SYNC, 0, null, null));
        }
        commitProcessor.commit(r);
    }

    @Override
    public int getGlobalOutstandingLimit() {
        int divisor = self.getQuorumSize() > 2 ? self.getQuorumSize() - 1 : 1;
        int globalOutstandingLimit = super.getGlobalOutstandingLimit() / divisor;
        return globalOutstandingLimit;
    }

    @Override
    public String getState() {
        return "follower";
    }

    @Override
    public Learner getLearner() {
        return getFollower();
    }

    /**
     * Process a request received from external Learner through the LearnerMaster
     * These requests have already passed through validation and checks for
     * session upgrade and can be injected into the middle of the pipeline.
     *
     * @param request received from external Learner
     */
    void processObserverRequest(Request request) {
        ((FollowerRequestProcessor) firstProcessor).processRequest(request, false);
    }

    boolean registerJMX(LearnerHandlerBean handlerBean) {
        try {
            MBeanRegistry.getInstance().register(handlerBean, jmxServerBean);
            return true;
        } catch (JMException e) {
            LOG.warn("Could not register connection", e);
        }
        return false;
    }

    @Override
    protected void registerMetrics() {
        super.registerMetrics();

        MetricsContext rootContext = ServerMetrics.getMetrics().getMetricsProvider().getRootContext();

        rootContext.registerGauge("synced_observers", self::getSynced_observers_metric);

    }

    @Override
    protected void unregisterMetrics() {
        super.unregisterMetrics();

        MetricsContext rootContext = ServerMetrics.getMetrics().getMetricsProvider().getRootContext();
        rootContext.unregisterGauge("synced_observers");

    }

}

```