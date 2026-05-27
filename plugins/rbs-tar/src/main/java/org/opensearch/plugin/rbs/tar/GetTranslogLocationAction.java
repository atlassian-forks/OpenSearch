/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.opensearch.action.ActionType;

public final class GetTranslogLocationAction extends ActionType<GetTranslogLocationResponse> {
    public static final GetTranslogLocationAction INSTANCE = new GetTranslogLocationAction();
    public static final String NAME = "indices:data/read/remote_store/translog/get_location";

    private GetTranslogLocationAction() {
        super(NAME, GetTranslogLocationResponse::new);
    }
}
