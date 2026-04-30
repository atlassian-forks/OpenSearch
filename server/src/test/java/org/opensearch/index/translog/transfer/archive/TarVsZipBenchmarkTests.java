/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Local micro-benchmark comparing TAR (single-pass streaming) vs ZIP (double-build) archive build times.
 *
 * NOT a JUnit correctness test — use this to validate the throughput improvement claim.
 * Run with: ./gradlew :server:test --tests "*TarVsZipBenchmarkTest*"
 *
 * Parameterised over:
 *   - numFiles: 2, 5, 10
 *   - fileSize: 50 KB, 100 KB, 200 KB
 */
public class TarVsZipBenchmarkTests extends OpenSearchTestCase {

    private static final int WARMUP_ITERATIONS = 50;
    private static final int BENCH_ITERATIONS  = 200;

    // ---- parameterised cases ----

    public void testBench_2files_50KB()  throws Exception { runBenchmark(2, 50 * 1024); }
    public void testBench_2files_100KB() throws Exception { runBenchmark(2, 100 * 1024); }
    public void testBench_2files_200KB() throws Exception { runBenchmark(2, 200 * 1024); }

    public void testBench_5files_50KB()  throws Exception { runBenchmark(5, 50 * 1024); }
    public void testBench_5files_100KB() throws Exception { runBenchmark(5, 100 * 1024); }
    public void testBench_5files_200KB() throws Exception { runBenchmark(5, 200 * 1024); }

    public void testBench_10files_50KB()  throws Exception { runBenchmark(10, 50 * 1024); }
    public void testBench_10files_100KB() throws Exception { runBenchmark(10, 100 * 1024); }
    public void testBench_10files_200KB() throws Exception { runBenchmark(10, 200 * 1024); }

    // ---- benchmark driver ----

    private void runBenchmark(int numFiles, int fileSizeBytes) throws Exception {
        List<ArchiveBuilder.ArchiveBuildEntry> entries = buildEntries(numFiles, fileSizeBytes);
        long totalDataMb = ((long) numFiles * fileSizeBytes) / (1024 * 1024);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            buildTar(entries);
            buildZip(entries);
        }

        // Benchmark TAR
        long tarNs = 0;
        for (int i = 0; i < BENCH_ITERATIONS; i++) {
            long t0 = System.nanoTime();
            buildTar(entries);
            tarNs += System.nanoTime() - t0;
        }
        double tarAvgMs = tarNs / 1e6 / BENCH_ITERATIONS;

        // Benchmark ZIP
        long zipNs = 0;
        for (int i = 0; i < BENCH_ITERATIONS; i++) {
            long t0 = System.nanoTime();
            buildZip(entries);
            zipNs += System.nanoTime() - t0;
        }
        double zipAvgMs = zipNs / 1e6 / BENCH_ITERATIONS;

        double speedup = zipAvgMs / tarAvgMs;

        System.out.printf(
            Locale.ROOT,
            "%n[BENCHMARK] files=%2d  fileSize=%6dKB  totalData=%4dMB | TAR=%6.2fms  ZIP=%6.2fms  speedup=%.2fx%n",
            numFiles, fileSizeBytes / 1024, totalDataMb, tarAvgMs, zipAvgMs, speedup
        );

        // TAR should be at least as fast as ZIP (usually significantly faster for larger files)
        // Allow up to 20% slower as a safety margin for CI noise
        assertTrue(
            String.format(Locale.ROOT,
                "TAR (%.2fms) should not be more than 20%% slower than ZIP (%.2fms)", tarAvgMs, zipAvgMs),
            tarAvgMs <= zipAvgMs * 1.2
        );
    }

    // ---- helpers ----

    private static List<ArchiveBuilder.ArchiveBuildEntry> buildEntries(int numFiles, int fileSizeBytes) {
        byte[] content = new byte[fileSizeBytes];
        // fill with pseudo-random data to avoid compression advantages
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i * 0x9e + 0x3b);
        }

        List<ArchiveBuilder.ArchiveBuildEntry> entries = new ArrayList<>(numFiles);
        for (int i = 0; i < numFiles; i++) {
            String path = String.format(Locale.ROOT, "uuid-bench/0/%d/translog-%d.%s",
                i / 2, i, (i % 2 == 0) ? "tlog" : "ckp");
            entries.add(ArchiveBuilder.fromBytes(path, content));
        }
        return entries;
    }

    private static byte[] buildTar(List<ArchiveBuilder.ArchiveBuildEntry> entries) throws IOException {
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) layout.getTotalSize());
        TarArchiveBuilder.build(out, layout, entries);
        return out.toByteArray();
    }

    private static byte[] buildZip(List<ArchiveBuilder.ArchiveBuildEntry> entries) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArchiveBuilder.buildWithComment(out, entries);
        return out.toByteArray();
    }
}
