package com.netease.nim.camellia.feign.lane;

/**
 * Lane route lookup request for a feign discovery service.
 */
public class FeignLaneRouteRequest {

    private final String laneId;
    private final String serviceName;
    private final long bid;
    private final String bgroup;
    private final Class<?> apiType;

    public FeignLaneRouteRequest(String laneId, String serviceName, long bid, String bgroup, Class<?> apiType) {
        this.laneId = laneId;
        this.serviceName = serviceName;
        this.bid = bid;
        this.bgroup = bgroup;
        this.apiType = apiType;
    }

    public String getLaneId() {
        return laneId;
    }

    public String getServiceName() {
        return serviceName;
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
}
