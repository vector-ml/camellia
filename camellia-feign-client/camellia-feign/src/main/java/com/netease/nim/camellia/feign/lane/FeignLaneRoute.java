package com.netease.nim.camellia.feign.lane;

/**
 * Lane route result for a feign discovery service.
 */
public class FeignLaneRoute {

    private boolean routeConfigured;
    private String fallback;
    private String target;

    public boolean isRouteConfigured() {
        return routeConfigured;
    }

    public void setRouteConfigured(boolean routeConfigured) {
        this.routeConfigured = routeConfigured;
    }

    public String getFallback() {
        return fallback;
    }

    public void setFallback(String fallback) {
        this.fallback = fallback;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

}
