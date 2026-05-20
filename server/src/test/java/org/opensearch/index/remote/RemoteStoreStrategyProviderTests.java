/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.index.translog.transfer.TranslogRemoteStoreStrategy;
import org.opensearch.plugins.RemoteStorePlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link RemoteStoreStrategyProvider}.
 *
 * <p>Covers the registry behavior: plugin discovery, name lookup, duplicate detection,
 * empty-name skipping, and the {@code otherTranslogStrategies()} fallback chain helper.
 */
public class RemoteStoreStrategyProviderTests extends OpenSearchTestCase {

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** A minimal RemoteStorePlugin stub with configurable name and strategies. */
    private static RemoteStorePlugin plugin(String name, SegmentRemoteStoreStrategy seg, TranslogRemoteStoreStrategy tlog) {
        return new RemoteStorePlugin() {
            @Override public String getStrategyName() { return name; }
            @Override public SegmentRemoteStoreStrategy getSegmentStrategy() { return seg; }
            @Override public TranslogRemoteStoreStrategy getTranslogStrategy() { return tlog; }
        };
    }

    private static SegmentRemoteStoreStrategy mockSeg() {
        return org.mockito.Mockito.mock(SegmentRemoteStoreStrategy.class);
    }

    private static TranslogRemoteStoreStrategy mockTlog() {
        return org.mockito.Mockito.mock(TranslogRemoteStoreStrategy.class);
    }

    // ── NOOP ─────────────────────────────────────────────────────────────────

    public void testNoopProviderReturnsNullForAllStrategies() {
        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.NOOP;
        assertNull(provider.segmentStrategyFor("tar"));
        assertNull(provider.translogStrategyFor("tar"));
        assertTrue(provider.otherTranslogStrategies("tar").isEmpty());
        assertTrue(provider.allTranslogStrategies().isEmpty());
        assertTrue(provider.getRegisteredNames().isEmpty());
    }

    public void testFromPluginsWithNullListReturnsNoop() {
        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(null);
        assertSame(RemoteStoreStrategyProvider.NOOP, provider);
    }

    public void testFromPluginsWithEmptyListReturnsNoop() {
        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(List.of());
        assertSame(RemoteStoreStrategyProvider.NOOP, provider);
    }

    // ── Single plugin registration ────────────────────────────────────────────

    public void testSinglePluginRegistered() {
        SegmentRemoteStoreStrategy seg = mockSeg();
        TranslogRemoteStoreStrategy tlog = mockTlog();
        RemoteStorePlugin p = plugin("tar", seg, tlog);

        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(List.of(p));

        assertSame(seg, provider.segmentStrategyFor("tar"));
        assertSame(tlog, provider.translogStrategyFor("tar"));
        assertEquals(List.of("tar"), provider.getRegisteredNames());
    }

    public void testUnknownNameReturnsNull() {
        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(
            List.of(plugin("tar", mockSeg(), mockTlog()))
        );
        assertNull("Unknown strategy name must return null", provider.segmentStrategyFor("zip"));
        assertNull("Unknown strategy name must return null", provider.translogStrategyFor("zip"));
    }

    public void testEmptyNameReturnsNull() {
        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(
            List.of(plugin("tar", mockSeg(), mockTlog()))
        );
        assertNull("Empty strategy name must return null (use default)", provider.segmentStrategyFor(""));
        assertNull("Empty strategy name must return null (use default)", provider.translogStrategyFor(""));
        assertNull("Null strategy name must return null", provider.segmentStrategyFor(null));
        assertNull("Null strategy name must return null", provider.translogStrategyFor(null));
    }

    // ── Plugin with empty getStrategyName() is skipped ────────────────────────

    public void testPluginWithEmptyStrategyNameIsSkipped() {
        RemoteStorePlugin badPlugin = plugin("", mockSeg(), mockTlog());
        RemoteStorePlugin goodPlugin = plugin("tar", mockSeg(), mockTlog());

        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(List.of(badPlugin, goodPlugin));

        assertEquals("Only the valid plugin must be registered", List.of("tar"), provider.getRegisteredNames());
    }

    // ── Duplicate strategy name is skipped ───────────────────────────────────

    public void testDuplicateStrategyNameIsSkipped() {
        SegmentRemoteStoreStrategy firstSeg = mockSeg();
        TranslogRemoteStoreStrategy firstTlog = mockTlog();
        RemoteStorePlugin first = plugin("tar", firstSeg, firstTlog);
        RemoteStorePlugin duplicate = plugin("tar", mockSeg(), mockTlog());

        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(List.of(first, duplicate));

        // Only the first registration should win
        assertSame("First registered segment strategy wins", firstSeg, provider.segmentStrategyFor("tar"));
        assertSame("First registered translog strategy wins", firstTlog, provider.translogStrategyFor("tar"));
        assertEquals("Only one entry for 'tar'", List.of("tar"), provider.getRegisteredNames());
    }

    // ── Multiple plugins ──────────────────────────────────────────────────────

    public void testMultiplePluginsRegistered() {
        SegmentRemoteStoreStrategy tarSeg = mockSeg();
        TranslogRemoteStoreStrategy tarTlog = mockTlog();
        SegmentRemoteStoreStrategy zipSeg = mockSeg();
        TranslogRemoteStoreStrategy zipTlog = mockTlog();

        RemoteStorePlugin tarPlugin = plugin("tar", tarSeg, tarTlog);
        RemoteStorePlugin zipPlugin = plugin("zip", zipSeg, zipTlog);

        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(List.of(tarPlugin, zipPlugin));

        assertSame(tarSeg, provider.segmentStrategyFor("tar"));
        assertSame(tarTlog, provider.translogStrategyFor("tar"));
        assertSame(zipSeg, provider.segmentStrategyFor("zip"));
        assertSame(zipTlog, provider.translogStrategyFor("zip"));
        assertEquals(List.of("tar", "zip"), provider.getRegisteredNames());
    }

    // ── Plugin providing null strategies ──────────────────────────────────────

    public void testPluginProvidingNullStrategiesIsSkippedFromAllLists() {
        RemoteStorePlugin nullBoth = plugin("nullboth", null, null);
        TranslogRemoteStoreStrategy tlog = mockTlog();
        RemoteStorePlugin withTlog = plugin("tar", null, tlog);

        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(List.of(nullBoth, withTlog));

        // "nullboth" is registered by name but not in allTranslogStrategies (null tlog)
        assertNull(provider.segmentStrategyFor("nullboth"));
        assertNull(provider.translogStrategyFor("nullboth"));
        assertSame(tlog, provider.translogStrategyFor("tar"));
        // allTranslogStrategies should only contain non-null ones
        assertEquals("Only non-null translog strategies in allTranslogStrategies", 1, provider.allTranslogStrategies().size());
        assertSame(tlog, provider.allTranslogStrategies().get(0));
    }

    // ── otherTranslogStrategies() fallback chain ──────────────────────────────

    public void testOtherTranslogStrategiesExcludesCurrent() {
        TranslogRemoteStoreStrategy tarTlog = mockTlog();
        TranslogRemoteStoreStrategy zipTlog = mockTlog();

        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(List.of(
            plugin("tar", null, tarTlog),
            plugin("zip", null, zipTlog)
        ));

        List<TranslogRemoteStoreStrategy> others = provider.otherTranslogStrategies("tar");
        assertEquals("Others must exclude current ('tar')", 1, others.size());
        assertSame(zipTlog, others.get(0));
    }

    public void testOtherTranslogStrategiesReturnsAllWhenCurrentIsNull() {
        TranslogRemoteStoreStrategy tarTlog = mockTlog();
        TranslogRemoteStoreStrategy zipTlog = mockTlog();

        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(List.of(
            plugin("tar", null, tarTlog),
            plugin("zip", null, zipTlog)
        ));

        List<TranslogRemoteStoreStrategy> others = provider.otherTranslogStrategies(null);
        assertEquals("Others with null current should return all", 2, others.size());
    }

    public void testOtherTranslogStrategiesReturnsAllWhenCurrentIsUnknown() {
        TranslogRemoteStoreStrategy tarTlog = mockTlog();
        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(
            List.of(plugin("tar", null, tarTlog))
        );

        List<TranslogRemoteStoreStrategy> others = provider.otherTranslogStrategies("nonexistent");
        assertEquals("Others with unknown current should return all", 1, others.size());
    }

    public void testOtherTranslogStrategiesIsEmptyWhenOnlyOnePlugin() {
        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(
            List.of(plugin("tar", null, mockTlog()))
        );

        List<TranslogRemoteStoreStrategy> others = provider.otherTranslogStrategies("tar");
        assertTrue("With only one plugin, others should be empty", others.isEmpty());
    }

    public void testAllTranslogStrategiesReturnedInRegistrationOrder() {
        TranslogRemoteStoreStrategy t1 = mockTlog();
        TranslogRemoteStoreStrategy t2 = mockTlog();
        TranslogRemoteStoreStrategy t3 = mockTlog();

        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(List.of(
            plugin("a", null, t1),
            plugin("b", null, t2),
            plugin("c", null, t3)
        ));

        List<TranslogRemoteStoreStrategy> all = provider.allTranslogStrategies();
        assertEquals(3, all.size());
        assertSame(t1, all.get(0));
        assertSame(t2, all.get(1));
        assertSame(t3, all.get(2));
    }

    // ── Backward compat deprecated methods ───────────────────────────────────

    @SuppressWarnings("deprecation")
    public void testDeprecatedGetSegmentStrategyReturnsFirstRegistered() {
        SegmentRemoteStoreStrategy first = mockSeg();
        RemoteStoreStrategyProvider provider = RemoteStoreStrategyProvider.fromPlugins(List.of(
            plugin("tar", first, null),
            plugin("zip", mockSeg(), null)
        ));
        assertSame("Deprecated getSegmentStrategy() must return first registered", first, provider.getSegmentStrategy());
    }

    @SuppressWarnings("deprecation")
    public void testDeprecatedGetSegmentStrategyReturnsNullWhenNoPlugins() {
        assertNull(RemoteStoreStrategyProvider.NOOP.getSegmentStrategy());
        assertNull(RemoteStoreStrategyProvider.NOOP.getTranslogStrategy());
    }
}
