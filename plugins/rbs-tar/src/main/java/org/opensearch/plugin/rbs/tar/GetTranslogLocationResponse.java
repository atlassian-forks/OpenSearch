/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class GetTranslogLocationResponse extends ActionResponse {
    private final List<NodeBundleRegistry.FileLocation> locations;

    public GetTranslogLocationResponse(final List<NodeBundleRegistry.FileLocation> locations) {
        this.locations = locations;
    }

    public GetTranslogLocationResponse(final StreamInput in) throws IOException {
        super(in);
        final int size = in.readVInt();
        this.locations = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            final boolean isTlg = in.readBoolean();
            final String bundlePath = in.readString();
            final long offset = in.readLong();
            final long length = in.readLong();
            final long generation = in.readLong();
            this.locations.add(new NodeBundleRegistry.FileLocation(isTlg, bundlePath, offset, length, generation));
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(locations.size());
        for (final NodeBundleRegistry.FileLocation loc : locations) {
            out.writeBoolean(loc.isTlg);
            out.writeString(loc.bundlePath);
            out.writeLong(loc.offset);
            out.writeLong(loc.length);
            out.writeLong(loc.generation);
        }
    }

    public List<NodeBundleRegistry.FileLocation> getLocations() {
        return locations;
    }
}
