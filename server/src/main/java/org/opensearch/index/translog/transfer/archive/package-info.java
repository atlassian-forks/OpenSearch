/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Translog archive format (ZIP stored) and directory parsing for per-node translog upload.
 * <ul>
 *   <li>Remote path: {@code basePath/archive/{nodeId}/{blobName}.zip}</li>
 *   <li>Member path inside ZIP: {@code indexUUID/shardId/primaryTerm/translog-<gen>.tlog|.ckp}</li>
 *   <li>Retention: delete entire archive only when all members are past retention (see {@link ArchiveDeletionHelper})</li>
 * </ul>
 *
 * @opensearch.internal
 */
package org.opensearch.index.translog.transfer.archive;
