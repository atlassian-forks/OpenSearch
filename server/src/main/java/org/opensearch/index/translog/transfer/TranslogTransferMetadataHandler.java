/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.io.IndexIOStreamHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Handler for {@link TranslogTransferMetadata}
 *
 * @opensearch.internal
 */
public class TranslogTransferMetadataHandler implements IndexIOStreamHandler<TranslogTransferMetadata> {

    /**
     * Implements logic to read content from file input stream {@code indexInput} and parse into {@link TranslogTransferMetadata}
     *
     * @param indexInput file input stream
     * @return content parsed to {@link TranslogTransferMetadata}
     */
    @Override
    public TranslogTransferMetadata readContent(IndexInput indexInput) throws IOException {
        long primaryTerm = indexInput.readLong();
        long generation = indexInput.readLong();
        long minTranslogGeneration = indexInput.readLong();
        Map<String, String> generationToPrimaryTermMapper = indexInput.readMapOfStrings();

        int count = generationToPrimaryTermMapper.size();
        TranslogTransferMetadata metadata = new TranslogTransferMetadata(primaryTerm, generation, minTranslogGeneration, count);
        metadata.setGenerationToPrimaryTermMapper(generationToPrimaryTermMapper);

        // v2: archive fields (optional — absent in v1 metadata files)
        if (indexInput.getFilePointer() < indexInput.length()) {
            String archiveBlobPath = indexInput.readString();
            if (!archiveBlobPath.isEmpty()) {
                metadata.setArchiveBlobPath(archiveBlobPath);
            }
            Map<String, String> archiveEntryOffsets = indexInput.readMapOfStrings();
            if (!archiveEntryOffsets.isEmpty()) {
                metadata.setArchiveEntryOffsets(archiveEntryOffsets);
            }
        }

        return metadata;
    }

    /**
     * Implements logic to write content from {@code content} to file output stream {@code indexOutput}
     *
     * @param indexOutput file input stream
     * @param content metadata content to be written
     */
    @Override
    public void writeContent(IndexOutput indexOutput, TranslogTransferMetadata content) throws IOException {
        indexOutput.writeLong(content.getPrimaryTerm());
        indexOutput.writeLong(content.getGeneration());
        indexOutput.writeLong(content.getMinTranslogGeneration());
        if (content.getGenerationToPrimaryTermMapper() != null) {
            indexOutput.writeMapOfStrings(content.getGenerationToPrimaryTermMapper());
        } else {
            indexOutput.writeMapOfStrings(new HashMap<>());
        }
        // v2: archive fields
        String archiveBlobPath = content.getArchiveBlobPath();
        indexOutput.writeString(archiveBlobPath != null ? archiveBlobPath : "");
        Map<String, String> archiveEntryOffsets = content.getArchiveEntryOffsets();
        if (archiveEntryOffsets != null) {
            indexOutput.writeMapOfStrings(archiveEntryOffsets);
        } else {
            indexOutput.writeMapOfStrings(new HashMap<>());
        }
    }
}
