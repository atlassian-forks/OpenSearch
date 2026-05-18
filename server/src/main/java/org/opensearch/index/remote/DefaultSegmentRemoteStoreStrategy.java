/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.common.util.UploadListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
// IndexShard is kept for the interface contract (upload signature)
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Default segment remote store strategy: uploads each file individually and defers download/GC
 * to {@link RemoteSegmentStoreDirectory}'s built-in behavior.
 * <p>
 * This is the built-in strategy when no {@link org.opensearch.plugins.RemoteStorePlugin} is loaded.
 *
 * @opensearch.internal
 */
public class DefaultSegmentRemoteStoreStrategy implements SegmentRemoteStoreStrategy {

    private static final Logger logger = LogManager.getLogger(DefaultSegmentRemoteStoreStrategy.class);

    private final RemoteSegmentTransferTracker segmentTracker;

    public DefaultSegmentRemoteStoreStrategy(RemoteSegmentTransferTracker segmentTracker) {
        this.segmentTracker = segmentTracker;
    }

    @Override
    public void upload(
        Collection<String> files,
        Map<String, Long> sizeMap,
        Directory storeDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        IndexShard shard,
        ActionListener<Void> listener
    ) {
        if (files.isEmpty()) {
            logger.debug("No new segments to upload in DefaultSegmentRemoteStoreStrategy");
            listener.onResponse(null);
            return;
        }

        logger.debug("Uploading {} segment files individually", files.size());

        // Low-priority upload is determined by RemoteStoreRefreshListener based on recovery state.
        // DefaultSegmentRemoteStoreStrategy always uses normal priority.
        boolean lowPriority = false;

        GroupedActionListener<Void> batchUploadListener = new GroupedActionListener<>(
            ActionListener.map(listener, resp -> null),
            files.size()
        );

        for (String src : files) {
            UploadListener statsListener = createStatsListener(sizeMap);
            ActionListener<Void> perFileListener = ActionListener.wrap(resp -> {
                statsListener.onSuccess(src);
                batchUploadListener.onResponse(resp);
            }, ex -> {
                logger.warn(() -> new ParameterizedMessage("Exception: [{}] while uploading segment file [{}]", ex, src), ex);
                if (ex instanceof CorruptIndexException && shard != null) {
                    shard.failShard(ex.getMessage(), ex);
                }
                statsListener.onFailure(src);
                batchUploadListener.onFailure(ex);
            });
            statsListener.beforeUpload(src);
            remoteDirectory.copyFrom(storeDirectory, src, IOContext.DEFAULT, perFileListener, lowPriority);
        }
    }

    /**
     * Download is handled by {@link RemoteSegmentStoreDirectory#openInput(String, IOContext)} directly.
     * This method should not be called.
     */
    @Override
    public IndexInput openInput(String name, UploadedSegmentMetadata metadata) throws IOException {
        throw new UnsupportedOperationException(
            "DefaultSegmentRemoteStoreStrategy.openInput() should not be called — "
                + "download is handled directly by RemoteSegmentStoreDirectory"
        );
    }

    /**
     * Returns {@link GcDecision#USE_DEFAULT}: core performs its normal LIST + delete GC.
     */
    @Override
    public GcDecision resolveStaleBlobs(Set<String> activeUploadedFilenames, long latestMetadataGeneration) {
        return GcDecision.USE_DEFAULT;
    }

    private UploadListener createStatsListener(Map<String, Long> sizeMap) {
        return new UploadListener() {
            private long uploadStartTime = 0;

            @Override
            public void beforeUpload(String f) {
                segmentTracker.addUploadBytesStarted(sizeMap.getOrDefault(f, 0L));
                uploadStartTime = System.currentTimeMillis();
            }

            @Override
            public void onSuccess(String f) {
                segmentTracker.addUploadBytesSucceeded(sizeMap.getOrDefault(f, 0L));
                segmentTracker.addToLatestUploadedFiles(f);
                segmentTracker.addUploadTimeInMillis(Math.max(1, System.currentTimeMillis() - uploadStartTime));
            }

            @Override
            public void onFailure(String f) {
                segmentTracker.addUploadBytesFailed(sizeMap.getOrDefault(f, 0L));
                segmentTracker.addUploadTimeInMillis(Math.max(1, System.currentTimeMillis() - uploadStartTime));
            }
        };
    }
}
