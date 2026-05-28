package com.netease.nim.camellia.feign.lane;

/**
 * Immutable lane routing context attached to one camellia-feign client.
 */
public class FeignLaneRouteContext {

    private final long bid;
    private final String bgroup;
    private final Class<?> apiType;
    private final FeignLaneIdProvider laneIdProvider;
    private final FeignLaneRouteResolver laneRouteResolver;

    public FeignLaneRouteContext(long bid, String bgroup, Class<?> apiType,
                                 FeignLaneIdProvider laneIdProvider,
                                 FeignLaneRouteResolver laneRouteResolver) {
        this.bid = bid;
        this.bgroup = bgroup;
        this.apiType = apiType;
        this.laneIdProvider = laneIdProvider;
        this.laneRouteResolver = laneRouteResolver;
    }

    public long getBid() {
        return bid;
    }

    public String getBgroup() {
        return bgroup;
    }

    public Class<?> getApiType() {
        return apiType;
    }

    public FeignLaneIdProvider getLaneIdProvider() {
        return laneIdProvider;
    }

    public FeignLaneRouteResolver getLaneRouteResolver() {
        return laneRouteResolver;
    }

    public boolean isAvailable() {
        return laneIdProvider != null && laneRouteResolver != null;
    }
}
