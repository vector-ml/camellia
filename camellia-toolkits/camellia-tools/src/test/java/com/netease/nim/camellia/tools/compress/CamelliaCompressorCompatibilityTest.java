package com.netease.nim.camellia.tools.compress;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

public class CamelliaCompressorCompatibilityTest {

    private static final String CLASS_NAME = "com.netease.nim.camellia.tools.compress.CamelliaCompressor";
    private static final String LEGACY_LZ4_JAR = "target/test-libs/org-lz4-java-1.8.0.jar";

    @Test
    public void camelliaCompressorShouldReadLegacyAndYawkData() throws Exception {
        CompressorAdapter legacy = newCompressor("legacy", new File(LEGACY_LZ4_JAR));
        CompressorAdapter yawk = newCompressor("yawk", locateClassJar(net.jpountz.lz4.LZ4Factory.class));

        Map<String, CompressorAdapter> compressors = new LinkedHashMap<String, CompressorAdapter>();
        compressors.put("org", legacy);
        compressors.put("yawk", yawk);

        for (byte[] sample : compressibleSamples()) {
            for (Map.Entry<String, CompressorAdapter> writer : compressors.entrySet()) {
                byte[] compressed = writer.getValue().compress(sample);
                Assert.assertFalse(writer.getKey() + " should compress sample", Arrays.equals(sample, compressed));
                for (Map.Entry<String, CompressorAdapter> reader : compressors.entrySet()) {
                    byte[] restored = reader.getValue().decompress(compressed);
                    Assert.assertArrayEquals(writer.getKey() + "->" + reader.getKey(), sample, restored);
                }
            }
        }
    }

    @Test
    public void currentCompressorShouldKeepBypassBehavior() {
        CamelliaCompressor compressor = new CamelliaCompressor();
        byte[] small = "small".getBytes(StandardCharsets.UTF_8);
        Assert.assertArrayEquals(small, compressor.compress(small));
        Assert.assertArrayEquals(small, compressor.decompress(small));

        byte[] invalid = "not-a-camellia-payload-but-long-enough".getBytes(StandardCharsets.UTF_8);
        Assert.assertArrayEquals(invalid, compressor.decompress(invalid));
    }

    private static CompressorAdapter newCompressor(String name, File lz4Jar) throws Exception {
        File classesDir = new File("target/classes");
        URL[] urls = new URL[] {
                classesDir.toURI().toURL(),
                lz4Jar.toURI().toURL(),
                locateClassJar(org.slf4j.Logger.class).toURI().toURL(),
                locateClassJar(org.slf4j.LoggerFactory.class).toURI().toURL()
        };
        ChildFirstClassLoader classLoader = new ChildFirstClassLoader(urls, CamelliaCompressorCompatibilityTest.class.getClassLoader());
        Class<?> type = Class.forName(CLASS_NAME, true, classLoader);
        Constructor<?> constructor = type.getConstructor(int.class);
        Object instance = constructor.newInstance(1);
        return new CompressorAdapter(name, classLoader, instance);
    }

    private static File locateClassJar(Class<?> type) throws Exception {
        String path = type.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
        return new File(path);
    }

    private static byte[][] compressibleSamples() {
        byte[] repeated = new byte[4096];
        Arrays.fill(repeated, (byte) 'a');

        byte[] pattern = new byte[4096];
        for (int i = 0; i < pattern.length; i++) {
            pattern[i] = (byte) ('a' + i % 8);
        }

        byte[] json = repeat("{\"appkey\":\"demo\",\"body\":\"hello lz4 compatibility\"}", 128)
                .getBytes(StandardCharsets.UTF_8);

        return new byte[][] { repeated, pattern, json };
    }

    private static String repeat(String value, int times) {
        StringBuilder builder = new StringBuilder(value.length() * times);
        for (int i = 0; i < times; i++) {
            builder.append(value);
        }
        return builder.toString();
    }

    private static final class CompressorAdapter {
        private final String name;
        private final URLClassLoader classLoader;
        private final Object instance;
        private final Method compress;
        private final Method decompress;

        private CompressorAdapter(String name, URLClassLoader classLoader, Object instance) throws Exception {
            this.name = name;
            this.classLoader = classLoader;
            this.instance = instance;
            this.compress = instance.getClass().getMethod("compress", byte[].class);
            this.decompress = instance.getClass().getMethod("decompress", byte[].class);
        }

        private byte[] compress(byte[] source) throws Exception {
            return (byte[]) compress.invoke(instance, source);
        }

        private byte[] decompress(byte[] source) throws Exception {
            return (byte[]) decompress.invoke(instance, source);
        }

        @Override
        public String toString() {
            return name + "@" + classLoader;
        }
    }

    private static final class ChildFirstClassLoader extends URLClassLoader {
        private ChildFirstClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("com.netease.nim.camellia.tools.compress.") || name.startsWith("net.jpountz.")) {
                synchronized (getClassLoadingLock(name)) {
                    Class<?> loaded = findLoadedClass(name);
                    if (loaded == null) {
                        try {
                            loaded = findClass(name);
                        } catch (ClassNotFoundException ignored) {
                            loaded = super.loadClass(name, false);
                        }
                    }
                    if (resolve) {
                        resolveClass(loaded);
                    }
                    return loaded;
                }
            }
            return super.loadClass(name, resolve);
        }
    }
}
