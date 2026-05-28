package com.netease.nim.camellia.feign.discovery;

import com.netease.nim.camellia.core.discovery.*;
import com.netease.nim.camellia.feign.GlobalCamelliaFeignEnv;
import com.netease.nim.camellia.feign.lane.FeignLaneIdProvider;
import com.netease.nim.camellia.feign.lane.FeignLaneRoute;
import com.netease.nim.camellia.feign.lane.FeignLaneRouteContext;
import com.netease.nim.camellia.feign.lane.FeignLaneRouteRequest;
import com.netease.nim.camellia.feign.resource.FeignDiscoveryResource;
import com.netease.nim.camellia.feign.resource.FeignResource;
import com.netease.nim.camellia.tools.cache.CamelliaLocalCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by caojiajun on 2022/3/1
 */
public class DiscoveryResourcePool implements FeignResourcePool {

    private static final Logger logger = LoggerFactory.getLogger(DiscoveryResourcePool.class);

    private final CamelliaDiscovery discovery;
    private final CamelliaServerSelector<FeignResource> serverSelector;
    private final CamelliaServerHealthChecker<FeignServerInfo> healthChecker;
    private final FeignDiscoveryResource discoveryResource;
    private final CamelliaDiscoveryFactory discoveryFactory;
    private final FeignLaneRouteContext laneRouteContext;
    private final ScheduledExecutorService scheduledExecutor;
    private final Map<String, LaneDiscoveryResourceCache> laneResourceCacheMap = new ConcurrentHashMap<>();
    private final Set<String> laneResourceUrls = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    private List<FeignResource> originalList = new ArrayList<>();
    private List<FeignResource> dynamicList = new ArrayList<>();

    private final Object lock = new Object();

    public DiscoveryResourcePool(FeignDiscoveryResource discoveryResource,
                                 CamelliaDiscovery discovery,
                                 CamelliaServerSelector<FeignResource> serverSelector,
                                 CamelliaServerHealthChecker<FeignServerInfo> healthChecker,
                                 ScheduledExecutorService scheduledExecutor) {
        this(discoveryResource, discovery, serverSelector, healthChecker, scheduledExecutor, null, null);
    }

    public DiscoveryResourcePool(FeignDiscoveryResource discoveryResource,
                                 CamelliaDiscovery discovery,
                                 CamelliaServerSelector<FeignResource> serverSelector,
                                 CamelliaServerHealthChecker<FeignServerInfo> healthChecker,
                                 ScheduledExecutorService scheduledExecutor,
                                 FeignLaneRouteContext laneRouteContext) {
        this(discoveryResource, discovery, serverSelector, healthChecker, scheduledExecutor, null, laneRouteContext);
    }

    public DiscoveryResourcePool(FeignDiscoveryResource discoveryResource,
                                 CamelliaDiscovery discovery,
                                 CamelliaServerSelector<FeignResource> serverSelector,
                                 CamelliaServerHealthChecker<FeignServerInfo> healthChecker,
                                 ScheduledExecutorService scheduledExecutor,
                                 CamelliaDiscoveryFactory discoveryFactory,
                                 FeignLaneRouteContext laneRouteContext) {
        this.discovery = discovery;
        this.serverSelector = serverSelector;
        this.discoveryResource = discoveryResource;
        this.healthChecker = healthChecker;
        this.discoveryFactory = discoveryFactory;
        this.laneRouteContext = laneRouteContext;
        this.scheduledExecutor = scheduledExecutor;
        if (discovery == null) {
            throw new IllegalArgumentException("discovery is empty");
        }
        if (serverSelector == null) {
            throw new IllegalArgumentException("serverSelector is empty");
        }
        reload();
        if (originalList.isEmpty()) {
            logger.warn("server list is empty, camellia-feign-service = {}", discoveryResource.getUrl());
        }
        discovery.setCallback(new CamelliaDiscovery.Callback() {
            @Override
            public void add(ServerNode server) {
                DiscoveryResourcePool.this.add(toFeignResource(server));
            }

            @Override
            public void remove(ServerNode server) {
                DiscoveryResourcePool.this.remove(toFeignResource(server));
            }
        });
        //兜底1分钟reload一次
        scheduledExecutor.scheduleAtFixedRate(this::reload, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    public FeignResource getResource(Object loadBalanceKey) {
        try {
            FeignResource laneResource = getLaneResource(loadBalanceKey);
            if (laneResource != null) {
                return laneResource;
            }
            Exception cause = null;
            int retry = 10;
            FeignResource feignResource = null;
            while (retry > 0) {
                retry --;
                try {
                    if (dynamicList.isEmpty()) {
                        feignResource = serverSelector.pick(originalList, loadBalanceKey);
                    } else {
                        feignResource = serverSelector.pick(dynamicList, loadBalanceKey);
                    }
                } catch (Exception e) {
                    cause = e;
                    continue;
                }
                if (feignResource != null) {
                    if (healthCheck(feignResource)) {
                        return feignResource;
                    }
                }
            }
            if (feignResource != null) {
                return feignResource;
            }
            throw new IllegalStateException("no reachable server", cause);
        } catch (Exception e) {
            throw new IllegalStateException("no reachable server", e);
        }
    }

    @Override
    public void onError(FeignResource feignResource) {
        try {
            if (isLaneResource(feignResource)) {
                return;
            }
            synchronized (lock) {
                Set<FeignResource> set = new HashSet<>(dynamicList);
                set.remove(feignResource);
                List<FeignResource> list = new ArrayList<>(set);
                Collections.sort(list);
                dynamicList = new ArrayList<>(list);
                if (dynamicList.isEmpty()) {
                    GlobalCamelliaFeignEnv.register(discoveryResource, serverSelector, new ArrayList<>(originalList));
                } else {
                    GlobalCamelliaFeignEnv.register(discoveryResource, serverSelector, new ArrayList<>(dynamicList));
                }
                if (GlobalDiscoveryEnv.logInfoEnable) {
                    logger.info("onError feignResource = {}, dynamicList = {}, originalList = {}", feignResource, dynamicList, originalList);
                }
            }
        } catch (Exception e) {
            logger.error("onError error", e);
        }
    }

    private void add(FeignResource feignResource) {
        try {
            synchronized (lock) {
                Set<FeignResource> set = new HashSet<>(originalList);
                set.add(feignResource);
                List<FeignResource> list = new ArrayList<>(set);
                Collections.sort(list);
                originalList = new ArrayList<>(list);
                dynamicList = new ArrayList<>(list);
                GlobalCamelliaFeignEnv.register(discoveryResource, serverSelector, new ArrayList<>(list));
                if (GlobalDiscoveryEnv.logInfoEnable) {
                    logger.info("add feignResource = {}, dynamicList = {}, originalList = {}", feignResource, dynamicList, originalList);
                }
            }
        } catch (Exception e) {
            logger.error("add error", e);
        }
    }

    private void remove(FeignResource feignResource) {
        try {
            synchronized (lock) {
                Set<FeignResource> set = new HashSet<>(originalList);
                set.remove(feignResource);
                if (set.isEmpty()) {
                    logger.warn("last server, skip remove");
                    return;
                }
                List<FeignResource> list = new ArrayList<>(set);
                Collections.sort(list);
                originalList = new ArrayList<>(list);
                dynamicList = new ArrayList<>(list);
                GlobalCamelliaFeignEnv.register(discoveryResource, serverSelector, new ArrayList<>(list));
                if (GlobalDiscoveryEnv.logInfoEnable) {
                    logger.info("remove feignResource = {}, dynamicList = {}, originalList = {}", feignResource, dynamicList, originalList);
                }
            }
        } catch (Exception e) {
            logger.error("remove error", e);
        }
    }

    private final CamelliaLocalCache cache = new CamelliaLocalCache();
    private static final String HEALTH_CHECK = "healthCheck";
    private static final String LANE_HEALTH_CHECK = "laneHealthCheck";

    private boolean healthCheck(FeignResource feignResource) {
        String url = feignResource.getUrl();
        Boolean result = cache.get(HEALTH_CHECK, url, Boolean.class);
        if (result == null) {
            result = healthChecker.healthCheck(toServerInfo(feignResource));
            //缓存个1s
            cache.put(HEALTH_CHECK, url, result, 1);
            return result;
        }
        return result;
    }

    private FeignResource getLaneResource(Object loadBalanceKey) {
        if (laneRouteContext == null || !laneRouteContext.isAvailable()) {
            return null;
        }
        String laneId = null;
        try {
            FeignLaneIdProvider laneIdProvider = laneRouteContext.getLaneIdProvider();
            laneId = laneIdProvider.getLaneId();
            if (isBlank(laneId)) {
                return null;
            }
            FeignLaneRouteRequest request = new FeignLaneRouteRequest(laneId, discoveryResource.getServiceName(),
                    laneRouteContext.getBid(), laneRouteContext.getBgroup(), laneRouteContext.getApiType());
            FeignLaneRoute route = laneRouteContext.getLaneRouteResolver().resolve(request);
            if (route == null || !route.isRouteConfigured()) {
                return null;
            }
            String target = route.getTarget();
            if (isBlank(target)) {
                if (isFailFallback(route.getFallback())) {
                    throw new IllegalStateException("lane feign route has no target, laneId = " + laneId
                            + ", serviceName = " + discoveryResource.getServiceName());
                }
                return null;
            }
            List<FeignResource> laneResources = findLaneResources(target);
            if (laneResources.isEmpty()) {
                if (isFailFallback(route.getFallback())) {
                    throw new IllegalStateException("lane feign route has no node, laneId = " + laneId
                            + ", serviceName = " + discoveryResource.getServiceName()
                            + ", target = " + target);
                }
                return null;
            }
            FeignResource feignResource = pickHealthyLaneResource(laneResources, loadBalanceKey);
            if (feignResource != null) {
                laneResourceUrls.add(feignResource.getUrl());
                return feignResource;
            }
            if (isFailFallback(route.getFallback())) {
                throw new IllegalStateException("lane feign route has no healthy node, laneId = " + laneId
                        + ", serviceName = " + discoveryResource.getServiceName()
                        + ", target = " + target);
            }
            return null;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            logger.warn("resolve lane feign route error, laneId = {}, serviceName = {}",
                    laneId, discoveryResource.getServiceName(), e);
            return null;
        }
    }

    private boolean isFailFallback(String fallback) {
        return "FAIL".equalsIgnoreCase(fallback);
    }

    private FeignResource pickHealthyLaneResource(List<FeignResource> laneResources, Object loadBalanceKey) {
        Exception cause = null;
        int retry = Math.max(1, laneResources.size());
        FeignResource feignResource = null;
        while (retry > 0) {
            retry--;
            try {
                feignResource = serverSelector.pick(laneResources, loadBalanceKey);
            } catch (Exception e) {
                cause = e;
                continue;
            }
            if (feignResource != null && laneHealthCheck(feignResource)) {
                return feignResource;
            }
        }
        if (cause != null && GlobalDiscoveryEnv.logInfoEnable) {
            logger.info("pick lane feign resource error, laneResources = {}", laneResources, cause);
        }
        return null;
    }

    private List<FeignResource> findLaneResources(String target) {
        if (discoveryFactory == null) {
            return Collections.emptyList();
        }
        LaneDiscoveryResourceCache resourceCache = getLaneResourceCache(target);
        if (resourceCache == null) {
            return Collections.emptyList();
        }
        return resourceCache.currentResources();
    }

    private LaneDiscoveryResourceCache getLaneResourceCache(String target) {
        String serviceName = target.trim();
        LaneDiscoveryResourceCache resourceCache = laneResourceCacheMap.get(serviceName);
        if (resourceCache != null) {
            return resourceCache;
        }
        synchronized (laneResourceCacheMap) {
            resourceCache = laneResourceCacheMap.get(serviceName);
            if (resourceCache != null) {
                return resourceCache;
            }
            CamelliaDiscovery laneDiscovery = discoveryFactory.getDiscovery(serviceName);
            if (laneDiscovery == null) {
                return null;
            }
            resourceCache = new LaneDiscoveryResourceCache(serviceName, laneDiscovery);
            laneResourceCacheMap.put(serviceName, resourceCache);
            return resourceCache;
        }
    }

    private boolean laneHealthCheck(FeignResource feignResource) {
        String url = feignResource.getUrl();
        Boolean result = cache.get(LANE_HEALTH_CHECK, url, Boolean.class);
        if (result == null) {
            result = healthChecker.healthCheck(toServerInfo(feignResource));
            cache.put(LANE_HEALTH_CHECK, url, result, 1);
            return result;
        }
        return result;
    }

    private boolean isLaneResource(FeignResource feignResource) {
        if (feignResource == null) {
            return false;
        }
        return laneResourceUrls.contains(feignResource.getUrl()) && !originalList.contains(feignResource);
    }

    private boolean isBlank(String string) {
        return string == null || string.trim().isEmpty();
    }

    private void reload() {
        List<ServerNode> all = discovery.findAll();
        List<FeignResource> list = new ArrayList<>();
        for (ServerNode serverNode : all) {
            list.add(toFeignResource(serverNode));
        }
        Collections.sort(list);
        this.originalList = new ArrayList<>(list);
        this.dynamicList = new ArrayList<>(list);

        GlobalCamelliaFeignEnv.register(discoveryResource, serverSelector, new ArrayList<>(list));
        if (GlobalDiscoveryEnv.logInfoEnable) {
            logger.info("reload, dynamicList = {}, originalList = {}", dynamicList, originalList);
        }
    }

    private FeignResource toFeignResource(ServerNode node) {
        return new FeignResource(discoveryResource.getProtocol() + node.getHost() + ":" + node.getPort());
    }

    private FeignServerInfo toServerInfo(FeignResource feignResource) {
        String feignUrl = feignResource.getFeignUrl();
        String[] split = feignUrl.substring(feignResource.getProtocol().length()).split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);
        FeignServerInfo feignServerInfo = new FeignServerInfo();
        feignServerInfo.setHost(host);
        feignServerInfo.setPort(port);
        return feignServerInfo;
    }

    private class LaneDiscoveryResourceCache {

        private final String serviceName;
        private final CamelliaDiscovery laneDiscovery;
        private final Object lock = new Object();
        private volatile List<FeignResource> resources = Collections.emptyList();

        private LaneDiscoveryResourceCache(String serviceName, CamelliaDiscovery laneDiscovery) {
            this.serviceName = serviceName;
            this.laneDiscovery = laneDiscovery;
            reload();
            laneDiscovery.setCallback(new CamelliaDiscovery.Callback() {
                @Override
                public void add(ServerNode server) {
                    LaneDiscoveryResourceCache.this.add(toFeignResource(server));
                }

                @Override
                public void remove(ServerNode server) {
                    LaneDiscoveryResourceCache.this.remove(toFeignResource(server));
                }
            });
            scheduledExecutor.scheduleAtFixedRate(this::reload, 1, 1, TimeUnit.MINUTES);
        }

        private List<FeignResource> currentResources() {
            return new ArrayList<>(resources);
        }

        private void add(FeignResource feignResource) {
            try {
                if (feignResource == null) {
                    return;
                }
                synchronized (lock) {
                    Set<FeignResource> set = new HashSet<>(resources);
                    set.add(feignResource);
                    List<FeignResource> list = new ArrayList<>(set);
                    Collections.sort(list);
                    resources = list;
                }
            } catch (Exception e) {
                logger.error("add lane feign resource error, serviceName = {}", serviceName, e);
            }
        }

        private void remove(FeignResource feignResource) {
            try {
                if (feignResource == null) {
                    return;
                }
                synchronized (lock) {
                    Set<FeignResource> set = new HashSet<>(resources);
                    set.remove(feignResource);
                    List<FeignResource> list = new ArrayList<>(set);
                    Collections.sort(list);
                    resources = list;
                }
            } catch (Exception e) {
                logger.error("remove lane feign resource error, serviceName = {}", serviceName, e);
            }
        }

        private void reload() {
            try {
                List<ServerNode> nodes = laneDiscovery.findAll();
                if (nodes == null || nodes.isEmpty()) {
                    resources = Collections.emptyList();
                    return;
                }
                List<FeignResource> list = new ArrayList<>();
                for (ServerNode node : nodes) {
                    if (node == null) {
                        continue;
                    }
                    list.add(toFeignResource(node));
                }
                Collections.sort(list);
                resources = list;
            } catch (Exception e) {
                logger.warn("reload lane feign resources error, serviceName = {}", serviceName, e);
            }
        }
    }
}
