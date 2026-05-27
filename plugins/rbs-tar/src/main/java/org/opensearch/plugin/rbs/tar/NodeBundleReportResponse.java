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

public final class NodeBundleReportResponse extends ActionResponse {
    private final boolean acknowledged;

    public NodeBundleReportResponse(final boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public NodeBundleReportResponse(final StreamInput in) throws IOException {
        super(in);
        this.acknowledged = in.readBoolean();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }
}
