/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.translog.transfer.archive.ArchiveIndexEntry;
import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class TranslogArchiveRecoveryTests extends OpenSearchTestCase {




    /** Builds a TAR archive containing tlog/ckp pairs for the given shards. */
    private byte[] buildTar(int[] shardIds, long[] generations, long[] primaryTerms) throws IOException {
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = new ArrayList<>();
        for (int i = 0; i < shardIds.length; i++) {
            String prefix = "indexUUID/" + shardIds[i] + "/" + primaryTerms[i] + "/";
            String tlogPath = prefix + "translog-" + generations[i] + ".tlog";
            String ckpPath = prefix + "translog-" + generations[i] + ".ckp";
            entries.add(
                TarArchiveBuilder.fromBytes(tlogPath, ("tlog-" + shardIds[i] + "-" + generations[i]).getBytes(StandardCharsets.UTF_8))
            );
            entries.add(TarArchiveBuilder.fromBytes(ckpPath, ("ckp-" + shardIds[i] + "-" + generations[i]).getBytes(StandardCharsets.UTF_8)));
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        TarArchiveBuilder.build(out, layout, entries);
        return out.toByteArray();
    }




    @SuppressWarnings("unchecked")
    private static Iterable<String> any() {
        return org.mockito.ArgumentMatchers.any();
    }

    // ── New hierarchical-path recovery tests ──────────────────────────────────

    /**
     * collectTarsFromHierarchicalPath returns empty list when txlog/ root has no day dirs.
     */
    public void testCollectTarsEmptyWhenNoData() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        Mockito.when(
            transferService.listFolders(ArgumentMatchers.any(BlobPath.class))
        ).thenReturn(Collections.emptySet());

        BlobPath txlogRoot = new BlobPath().add("base").add("txlog");
        Instant startFrom = Instant.parse("2026-05-01T22:00:00Z");

        List<TranslogArchiveRecovery.ZipRef> result =
            TranslogArchiveRecovery.collectTarsFromHierarchicalPath(transferService, txlogRoot, startFrom);
        assertTrue("Should be empty when no day dirs", result.isEmpty());
    }

    /**
     * collectTarsFromHierarchicalPath skips day-dirs before startFrom.
     */
    public void testCollectTarsSkipsOldDays() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath txlogRoot = new BlobPath().add("base").add("txlog");

        // listFolders(txlogRoot) → {"20260430", "20260501"}
        Mockito.when(
            transferService.listFolders(txlogRoot)
        ).thenReturn(new HashSet<>(Arrays.asList("20260430", "20260501")));

        // listFolders for "20260430" day (old day) — should not be called since it's before startDay
        // listFolders for "20260501" → {"2230"}
        BlobPath dayPath = txlogRoot.add("20260501");
        Mockito.when(
            transferService.listFolders(dayPath)
        ).thenReturn(Collections.singleton("2230"));

        // listAllInSortedOrder for minute dir → empty
        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(Collections.emptyList());
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(dayPath.add("2230")),
            ArgumentMatchers.eq(""),
            ArgumentMatchers.anyInt(),
            ArgumentMatchers.any()
        );

        // startFrom is 2026-05-01T22:29:00Z → startDay="20260501", startMinute="2229"
        Instant startFrom = Instant.parse("2026-05-01T22:29:00Z");
        List<TranslogArchiveRecovery.ZipRef> result =
            TranslogArchiveRecovery.collectTarsFromHierarchicalPath(transferService, txlogRoot, startFrom);

        // Should not have called listFolders for the old day
        Mockito.verify(transferService, Mockito.never())
            .listFolders(txlogRoot.add("20260430"));
        assertTrue("Should return empty since minute dir has no blobs", result.isEmpty());
    }

    /**
     * readTarIndexCachedEntries caches on second call (returns same list instance).
     * Pre-populates cache to verify hit path without needing real TAR bytes.
     */
    public void testReadTarIndexCachedEntriesCache() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath blobPath = new BlobPath().add("txlog").add("20260501").add("2230");
        String blobName = "45.123.nodetest.tar";

        TranslogArchiveRecovery.ZipRef tarRef =
            new TranslogArchiveRecovery.ZipRef(blobPath, blobName, 100L);

        // Pre-populate cache with a sentinel list
        Map<String, List<ArchiveIndexEntry>> cache = new HashMap<>();
        String cacheKey = blobPath.buildAsString() + "/" + blobName;
        List<ArchiveIndexEntry> sentinel = Collections.emptyList();
        cache.put(cacheKey, sentinel);

        // First call: should hit cache, not call transferService
        List<ArchiveIndexEntry> result =
            TranslogArchiveRecovery.readTarIndexCachedEntries(transferService, tarRef, cache);

        assertSame("Should return cached list", sentinel, result);
        // TransferService should NOT have been called
        Mockito.verifyNoInteractions(transferService);

        // Second call: same result
        List<ArchiveIndexEntry> result2 =
            TranslogArchiveRecovery.readTarIndexCachedEntries(transferService, tarRef, cache);
        assertSame("Second call should also return cached instance", sentinel, result2);
        Mockito.verifyNoInteractions(transferService);
    }

    /**
     * RECOVERY_START_MARGIN_MINUTES is 2.
     */
    public void testRecoveryStartMargin() {
        assertEquals(2, TranslogArchiveRecovery.RECOVERY_START_MARGIN_MINUTES);
    }
}
