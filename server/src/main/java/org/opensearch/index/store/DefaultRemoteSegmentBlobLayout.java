/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache License, Version 2.0 or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.UUIDs;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class DefaultRemoteSegmentBlobLayout implements RemoteSegmentBlobLayout {

    private final RemoteSegmentBlobStore remoteStore;

    DefaultRemoteSegmentBlobLayout(RemoteSegmentBlobStore remoteStore) {
        this.remoteStore = remoteStore;
    }

    @Override
    public void writeBatch(WriteContext context, ActionListener<Map<String, String>> listener) {
        if (context.getFiles().isEmpty()) {
            listener.onResponse(Map.of());
            return;
        }
        Map<String, String> locations = new ConcurrentHashMap<>();
        AtomicInteger pending = new AtomicInteger(context.getFiles().size());
        AtomicBoolean completed = new AtomicBoolean();
        for (String file : context.getFiles()) {
            String location = file + RemoteSegmentStoreDirectory.SEGMENT_NAME_UUID_SEPARATOR + UUIDs.base64UUID();
            ActionListener<Void> fileListener = ActionListener.wrap(ignored -> {
                locations.put(file, location);
                if (pending.decrementAndGet() == 0 && completed.compareAndSet(false, true)) {
                    listener.onResponse(locations);
                }
            }, exception -> {
                if (completed.compareAndSet(false, true)) {
                    listener.onFailure(exception);
                }
            });
            try {
                boolean asynchronous = remoteStore.copyFrom(
                    context.getSourceDirectory(),
                    file,
                    location,
                    context.getIoContext(),
                    () -> {},
                    fileListener,
                    context.isLowPriorityUpload(),
                    context.getCryptoMetadata()
                );
                if (asynchronous == false) {
                    remoteStore.copyFrom(context.getSourceDirectory(), file, location, context.getIoContext());
                    fileListener.onResponse(null);
                }
            } catch (Exception e) {
                if (completed.compareAndSet(false, true)) {
                    listener.onFailure(e);
                }
                return;
            }
        }
    }

    @Override
    public IndexInput openInput(String physicalLocation, long logicalLength, IOContext context) throws IOException {
        return remoteStore.openInput(physicalLocation, logicalLength, context);
    }

    @Override
    public IndexInput openBlockInput(String physicalLocation, long position, long length, long logicalLength, IOContext context)
        throws IOException {
        return remoteStore.openBlockInput(physicalLocation, position, length, logicalLength, context);
    }

    @Override
    public void releaseLogicalFile(String physicalLocation) throws IOException {
        remoteStore.deleteFile(physicalLocation);
    }

    @Override
    public void deleteUnreferenced(Collection<String> stalePhysicalLocations, Set<String> activePhysicalLocations) throws IOException {
        List<String> locationsToDelete = new ArrayList<>();
        for (String location : stalePhysicalLocations) {
            if (activePhysicalLocations.contains(location) == false) {
                locationsToDelete.add(location);
            }
        }
        remoteStore.deleteFiles(locationsToDelete);
    }
}
