package com.netease.nim.camellia.feign.discovery;

import com.netease.nim.camellia.core.discovery.CamelliaDiscovery;
import com.netease.nim.camellia.core.discovery.CamelliaDiscoveryFactory;
import com.netease.nim.camellia.core.discovery.CamelliaServerHealthChecker;
import com.netease.nim.camellia.core.discovery.CamelliaServerSelector;
import com.netease.nim.camellia.core.discovery.ServerNode;
import com.netease.nim.camellia.feign.lane.FeignLaneRoute;
import com.netease.nim.camellia.feign.lane.FeignLaneRouteContext;
import com.netease.nim.camellia.feign.resource.FeignDiscoveryResource;
import com.netease.nim.camellia.feign.resource.FeignResource;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class DiscoveryResourcePoolLaneTest {

    @Test
    public void testLaneRouteUsesTargetDiscovery() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            Map<String, CamelliaDiscovery> discoveries = new ConcurrentHashMap<>();
            discoveries.put("public-service", new LocalDiscovery(new ServerNode("10.0.0.1", 8080)));
            discoveries.put("lane-service", new LocalDiscovery(new ServerNode("10.0.0.2", 8081)));

            DiscoveryResourcePool pool = newPool(executor, discoveries, "lane-1", route("lane-service", "PUBLIC"));

            FeignResource resource = pool.getResource(null);

            Assert.assertEquals("http://10.0.0.2:8081", resource.getFeignUrl());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testLaneRouteDoesNotFindAllOnEveryRequest() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            LocalDiscovery laneDiscovery = new LocalDiscovery(new ServerNode("10.0.0.2", 8081));
            Map<String, CamelliaDiscovery> discoveries = new ConcurrentHashMap<>();
            discoveries.put("public-service", new LocalDiscovery(new ServerNode("10.0.0.1", 8080)));
            discoveries.put("lane-service", laneDiscovery);

            DiscoveryResourcePool pool = newPool(executor, discoveries, "lane-1", route("lane-service", "PUBLIC"));

            Assert.assertEquals("http://10.0.0.2:8081", pool.getResource(null).getFeignUrl());
            Assert.assertEquals("http://10.0.0.2:8081", pool.getResource(null).getFeignUrl());

            Assert.assertEquals(1, laneDiscovery.findAllCount());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testLaneRouteCacheUpdatedByDiscoveryCallback() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            LocalDiscovery laneDiscovery = new LocalDiscovery(new ServerNode("10.0.0.2", 8081));
            Map<String, CamelliaDiscovery> discoveries = new ConcurrentHashMap<>();
            discoveries.put("public-service", new LocalDiscovery(new ServerNode("10.0.0.1", 8080)));
            discoveries.put("lane-service", laneDiscovery);

            DiscoveryResourcePool pool = newPool(executor, discoveries, "lane-1", route("lane-service", "PUBLIC"));

            Assert.assertEquals("http://10.0.0.2:8081", pool.getResource(null).getFeignUrl());
            laneDiscovery.callbackRemove(new ServerNode("10.0.0.2", 8081));
            laneDiscovery.callbackAdd(new ServerNode("10.0.0.3", 8082));

            Assert.assertEquals("http://10.0.0.3:8082", pool.getResource(null).getFeignUrl());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testNoLaneIdFallbackToPublicService() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            Map<String, CamelliaDiscovery> discoveries = new ConcurrentHashMap<>();
            discoveries.put("public-service", new LocalDiscovery(new ServerNode("10.0.0.1", 8080)));

            DiscoveryResourcePool pool = newPool(executor, discoveries, null, route("lane-service", "PUBLIC"));

            FeignResource resource = pool.getResource(null);

            Assert.assertEquals("http://10.0.0.1:8080", resource.getFeignUrl());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testEmptyLaneServiceFallbackToPublicService() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            Map<String, CamelliaDiscovery> discoveries = new ConcurrentHashMap<>();
            discoveries.put("public-service", new LocalDiscovery(new ServerNode("10.0.0.1", 8080)));
            discoveries.put("lane-service", new LocalDiscovery());

            DiscoveryResourcePool pool = newPool(executor, discoveries, "lane-1", route("lane-service", "PUBLIC"));

            FeignResource resource = pool.getResource(null);

            Assert.assertEquals("http://10.0.0.1:8080", resource.getFeignUrl());
        } finally {
            executor.shutdownNow();
        }
    }

    private DiscoveryResourcePool newPool(ScheduledExecutorService executor, Map<String, CamelliaDiscovery> discoveries,
                                          String laneId, FeignLaneRoute route) {
        CamelliaDiscoveryFactory discoveryFactory = discoveries::get;
        CamelliaServerSelector<FeignResource> selector = (list, loadBalanceKey) -> list == null || list.isEmpty() ? null : list.get(0);
        CamelliaServerHealthChecker<FeignServerInfo> healthChecker = server -> true;
        FeignLaneRouteContext laneRouteContext = new FeignLaneRouteContext(1L, "default", DiscoveryResourcePoolLaneTest.class,
                () -> laneId, request -> route);
        return new DiscoveryResourcePool(new FeignDiscoveryResource("http://public-service"),
                discoveries.get("public-service"), selector, healthChecker, executor, discoveryFactory, laneRouteContext);
    }

    @Test(expected = IllegalStateException.class)
    public void testFailFallbackEmptyLaneServiceFailFast() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            Map<String, CamelliaDiscovery> discoveries = new ConcurrentHashMap<>();
            discoveries.put("public-service", new LocalDiscovery(new ServerNode("10.0.0.1", 8080)));
            discoveries.put("lane-service", new LocalDiscovery());

            DiscoveryResourcePool pool = newPool(executor, discoveries, "lane-1", route("lane-service", "FAIL"));

            pool.getResource(null);
        } finally {
            executor.shutdownNow();
        }
    }

    private FeignLaneRoute route(String target, String fallback) {
        FeignLaneRoute route = new FeignLaneRoute();
        route.setRouteConfigured(true);
        route.setFallback(fallback);
        route.setTarget(target);
        return route;
    }

    private static class LocalDiscovery implements CamelliaDiscovery {

        private final List<ServerNode> nodes;
        private final AtomicInteger findAllCount = new AtomicInteger();
        private Callback callback;

        private LocalDiscovery(ServerNode... nodes) {
            this.nodes = nodes == null || nodes.length == 0 ? Collections.emptyList() : Arrays.asList(nodes);
        }

        @Override
        public List<ServerNode> findAll() {
            findAllCount.incrementAndGet();
            return nodes;
        }

        private int findAllCount() {
            return findAllCount.get();
        }

        private void callbackAdd(ServerNode node) {
            if (callback != null) {
                callback.add(node);
            }
        }

        private void callbackRemove(ServerNode node) {
            if (callback != null) {
                callback.remove(node);
            }
        }

        @Override
        public void setCallback(Callback callback) {
            this.callback = callback;
        }

        @Override
        public void clearCallback(Callback callback) {
            if (this.callback == callback) {
                this.callback = null;
            }
        }
    }
}
