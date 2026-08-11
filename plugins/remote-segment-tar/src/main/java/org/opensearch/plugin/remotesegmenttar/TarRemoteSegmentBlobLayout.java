/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.remotesegmenttar;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.UUIDs;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.store.RemoteSegmentBlobLayout;
import org.opensearch.index.store.RemoteSegmentBlobStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Stores one refresh-selected segment batch in one immutable standard TAR object.
 *
 * <p>The archive contains a versioned {@code index.bin} manifest followed by the selected logical files in their core
 * supplied order. The manifest records each logical name, data offset, logical length, and checksum. Core stores only
 * an opaque {@code tar:v1:<archive>#<offset>:<length>} location in normal remote segment metadata. It does not need to
 * understand TAR headers or shared-object ownership.
 *
 * <p>Write success means the complete archive is present in remote storage and there is one location for every input
 * file. Only then can core register those locations and publish its existing metadata/checkpoint sequence. A failed
 * write returns no locations. The temporary local archive is removed in either case; a failed remote upload also makes
 * a best-effort attempt to remove its unreferenced remote archive.
 *
 * <p>Logical files in the same archive share one physical object. {@link #releaseLogicalFile(String)} therefore does
 * not delete immediately. {@link #deleteUnreferenced(Collection, Set)} performs archive-level garbage collection from
 * core's retained metadata sets.
 */
final class TarRemoteSegmentBlobLayout implements RemoteSegmentBlobLayout {

    private static final String ARCHIVE_PREFIX = "rbs_segment_bundle__";
    private static final String ARCHIVE_SUFFIX = ".tar";
    private static final String MANIFEST_NAME = "index.bin";
    private static final int BUFFER_SIZE = 16 * 1024;

    private final RemoteSegmentBlobStore remoteStore;

    TarRemoteSegmentBlobLayout(RemoteSegmentBlobStore remoteStore) {
        this.remoteStore = remoteStore;
    }

    @Override
    public void writeBatch(WriteContext context, ActionListener<Map<String, String>> listener) {
        if (context.getFiles().isEmpty()) {
            listener.onResponse(Map.of());
            return;
        }
        String temporaryArchive = null;
        try {
            // Offsets include the TAR header that immediately precedes each logical file's data bytes.
            List<TarManifest.Entry> entries = createManifestEntries(context.getSourceDirectory(), context.getFiles());
            byte[] manifest = TarManifest.encode(entries);
            // Build locally first so RemoteSegmentBlobStore can retain its normal encrypted, rate-limited upload path.
            temporaryArchive = writeArchive(context.getSourceDirectory(), context.getFiles(), manifest);
            String archive = ARCHIVE_PREFIX + UUIDs.base64UUID() + ARCHIVE_SUFFIX;
            Map<String, String> locations = locations(entries, archive);
            String archiveToDelete = temporaryArchive;
            boolean asynchronous = remoteStore.copyFrom(
                context.getSourceDirectory(),
                temporaryArchive,
                archive,
                context.getIoContext(),
                () -> {},
                ActionListener.wrap(ignored -> {
                    // The remote archive is complete. Core can now make its opaque locations visible in metadata.
                    deleteTemporary(context.getSourceDirectory(), archiveToDelete);
                    listener.onResponse(locations);
                }, exception -> {
                    deleteTemporary(context.getSourceDirectory(), archiveToDelete);
                    deleteArchiveAfterFailure(archive);
                    listener.onFailure(exception);
                }),
                context.isLowPriorityUpload(),
                context.getCryptoMetadata()
            );
            if (asynchronous == false) {
                // Preserve RemoteDirectory's legacy synchronous fallback for repositories without multi-stream upload.
                remoteStore.copyFrom(context.getSourceDirectory(), temporaryArchive, archive, context.getIoContext());
                deleteTemporary(context.getSourceDirectory(), temporaryArchive);
                listener.onResponse(locations);
            }
        } catch (Exception exception) {
            if (temporaryArchive != null) {
                deleteTemporary(context.getSourceDirectory(), temporaryArchive);
            }
            listener.onFailure(exception);
        }
    }

    @Override
    public IndexInput openInput(String physicalLocation, long logicalLength, IOContext context) throws IOException {
        TarLocation location = validateLocation(physicalLocation, logicalLength);
        // Fetch only this logical file's data range, not the TAR header, manifest, or sibling entries.
        return remoteStore.openBlockInput(
            location.archive(),
            location.offset(),
            location.length(),
            location.offset() + location.length(),
            context
        );
    }

    @Override
    public IndexInput openBlockInput(String physicalLocation, long position, long length, long logicalLength, IOContext context)
        throws IOException {
        if (position < 0 || length < 0 || position + length > logicalLength) {
            throw new IOException("Invalid TAR range");
        }
        TarLocation location = validateLocation(physicalLocation, logicalLength);
        // Translate the logical offset into an absolute archive offset before requesting the rate-limited range.
        return remoteStore.openBlockInput(
            location.archive(),
            location.offset() + position,
            length,
            location.offset() + position + length,
            context
        );
    }

    @Override
    public void releaseLogicalFile(String physicalLocation) {
        // An archive can contain other logical files. Metadata-driven GC decides when it is safe to delete.
    }

    @Override
    public void deleteUnreferenced(Collection<String> stalePhysicalLocations, Set<String> activePhysicalLocations) throws IOException {
        // Core compares logical references. Convert both sets to archive names before making a physical deletion decision.
        Set<String> activeArchives = archives(activePhysicalLocations);
        Set<String> staleArchives = archives(stalePhysicalLocations);
        List<String> archivesToDelete = new ArrayList<>();
        for (String archive : staleArchives) {
            if (activeArchives.contains(archive) == false) {
                archivesToDelete.add(archive);
            }
        }
        remoteStore.deleteFiles(archivesToDelete);
    }

    private List<TarManifest.Entry> createManifestEntries(Directory directory, Collection<String> files) throws IOException {
        List<TarManifest.Entry> entries = new ArrayList<>();
        for (String file : files) {
            entries.add(new TarManifest.Entry(file, 0L, directory.fileLength(file), checksum(directory, file)));
        }
        byte[] manifest = TarManifest.encode(entries);
        for (int iteration = 0; iteration < 4; iteration++) {
            // Manifest entries contain decimal offsets. Re-encode until their serialized width no longer changes offsets.
            long position = roundedEntryLength(manifest.length);
            List<TarManifest.Entry> positioned = new ArrayList<>();
            for (TarManifest.Entry entry : entries) {
                long offset = position + TarArchive.HEADER_LENGTH;
                positioned.add(new TarManifest.Entry(entry.name(), offset, entry.length(), entry.checksum()));
                position += TarArchive.entryLength(entry.length());
            }
            byte[] candidate = TarManifest.encode(positioned);
            entries = positioned;
            if (candidate.length == manifest.length) {
                return entries;
            }
            manifest = candidate;
        }
        return entries;
    }

    private String writeArchive(Directory directory, Collection<String> files, byte[] manifest) throws IOException {
        try (IndexOutput output = directory.createTempOutput(ARCHIVE_PREFIX, ARCHIVE_SUFFIX, IOContext.DEFAULT)) {
            String temporaryName = output.getName();
            // Keeping the manifest first lets a reader identify the archive format without scanning segment entries.
            TarArchive.writeEntry(output, MANIFEST_NAME, manifest);
            for (String file : files) {
                // TarArchive copies from the Lucene directory in bounded buffers and does not aggregate files in memory.
                TarArchive.writeEntry(output, file, directory, file, BUFFER_SIZE);
            }
            TarArchive.finish(output);
            return temporaryName;
        }
    }

    private Map<String, String> locations(List<TarManifest.Entry> entries, String archive) {
        Map<String, String> locations = new HashMap<>();
        for (TarManifest.Entry entry : entries) {
            locations.put(entry.name(), new TarLocation(archive, entry.offset(), entry.length()).encode());
        }
        return Map.copyOf(locations);
    }

    private TarLocation validateLocation(String physicalLocation, long logicalLength) throws IOException {
        TarLocation location = TarLocation.parse(physicalLocation);
        if (location.length() != logicalLength) {
            throw new IOException("TAR location length does not match segment metadata");
        }
        return location;
    }

    private String checksum(Directory directory, String file) throws IOException {
        try (IndexInput input = directory.openInput(file, IOContext.READONCE)) {
            return Long.toString(CodecUtil.retrieveChecksum(input));
        }
    }

    private long roundedEntryLength(long length) {
        return TarArchive.entryLength(length);
    }

    private Set<String> archives(Collection<String> locations) throws IOException {
        Set<String> archives = new HashSet<>();
        for (String location : locations) {
            archives.add(TarLocation.parse(location).archive());
        }
        return archives;
    }

    private void deleteTemporary(Directory directory, String file) {
        try {
            directory.deleteFile(file);
        } catch (IOException ignored) {
            // The next directory cleanup can remove a local temporary archive left after a failed cleanup.
        }
    }

    private void deleteArchiveAfterFailure(String archive) {
        try {
            remoteStore.deleteFile(archive);
        } catch (IOException ignored) {
            // This is best effort. Core never received locations, so the archive is unreferenced and eligible for cleanup.
        }
    }
}
