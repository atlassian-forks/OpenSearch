/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.opensearch.action.ActionType;

public final class NodeBundleReportAction extends ActionType<NodeBundleReportResponse> {
    public static final NodeBundleReportAction INSTANCE = new NodeBundleReportAction();
    public static final String NAME = "internal:indices/remote_store/translog/bundle_report";

    private NodeBundleReportAction() {
        super(NAME, NodeBundleReportResponse::new);
    }
}
