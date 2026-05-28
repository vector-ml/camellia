package com.netease.nim.camellia.feign.lane;

/**
 * Provides the current request lane id for camellia-feign.
 */
public interface FeignLaneIdProvider {

    String getLaneId();
}
