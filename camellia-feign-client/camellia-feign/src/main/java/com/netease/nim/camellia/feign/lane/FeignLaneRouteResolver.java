package com.netease.nim.camellia.feign.lane;

/**
 * Resolves a lane-specific feign discovery service from an external routing control plane.
 */
public interface FeignLaneRouteResolver {

    FeignLaneRoute resolve(FeignLaneRouteRequest request);
}
