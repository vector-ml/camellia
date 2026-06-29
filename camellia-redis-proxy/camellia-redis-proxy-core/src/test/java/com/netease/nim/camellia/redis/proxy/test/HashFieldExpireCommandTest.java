package com.netease.nim.camellia.redis.proxy.test;

import com.netease.nim.camellia.redis.proxy.command.Command;
import com.netease.nim.camellia.redis.proxy.enums.RedisCommand;
import com.netease.nim.camellia.redis.proxy.util.KeyParser;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Verifies the redis hash field expiration commands (HEXPIRE family, redis 7.4+;
 * HGETEX/HSETEX/HGETDEL, redis 8.0+) are recognized by the proxy and routed as
 * single-key commands (key at argument index 1), so the trailing FIELDS clause
 * is never mistaken for keys.
 */
public class HashFieldExpireCommandTest {

    private static final String[] NAMES = {
            "hexpire", "hpexpire", "hexpireat", "hpexpireat", "hpersist",
            "httl", "hpttl", "hexpiretime", "hpexpiretime",
            "hgetex", "hsetex", "hgetdel"
    };

    @Test
    public void testCommandsRecognizedAsHash() {
        for (String name : NAMES) {
            RedisCommand command = RedisCommand.getSupportRedisCommandByName(name);
            assertNotNull("command not supported by proxy: " + name, command);
            assertEquals(name, RedisCommand.CommandType.HASH, command.getCommandType());
            assertEquals(name, RedisCommand.CommandKeyType.SIMPLE_SINGLE, command.getCommandKeyType());
        }
    }

    @Test
    public void testSingleKeyRouting() {
        // HEXPIRE key seconds FIELDS numfields field [field ...] -> only key is objects[1]
        byte[][] hexpire = {RedisCommand.HEXPIRE.raw(), toBytes("user:{1}:embedding"),
                toBytes("604800"), toBytes("FIELDS"), toBytes("1"), toBytes("soe_v1")};
        assertSingleKey("user:{1}:embedding", new Command(hexpire));

        // HSETEX key seconds FIELDS numfields field value [field value ...] -> only key is objects[1]
        byte[][] hsetex = {RedisCommand.HSETEX.raw(), toBytes("user:{2}:embedding"),
                toBytes("604800"), toBytes("FIELDS"), toBytes("1"), toBytes("soe_v1"), toBytes("val")};
        assertSingleKey("user:{2}:embedding", new Command(hsetex));
    }

    private static void assertSingleKey(String expectedKey, Command command) {
        List<byte[]> keys = KeyParser.findKeys(command);
        assertEquals(1, keys.size());
        assertEquals(expectedKey, new String(keys.get(0), StandardCharsets.UTF_8));
    }

    private static byte[] toBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
