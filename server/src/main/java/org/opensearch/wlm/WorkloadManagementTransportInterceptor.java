/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

/**
 * This class is used to intercept search traffic requests and populate the workloadGroupId header in task headers
 */
public class WorkloadManagementTransportInterceptor implements TransportInterceptor {
    private final ThreadPool threadPool;
    private final WorkloadGroupService workloadGroupService;

    public WorkloadManagementTransportInterceptor(final ThreadPool threadPool, final WorkloadGroupService workloadGroupService) {
        this.threadPool = threadPool;
        this.workloadGroupService = workloadGroupService;
    }

    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
        String action,
        String executor,
        boolean forceExecution,
        TransportRequestHandler<T> actualHandler
    ) {
        return new RequestHandler<T>(threadPool, actualHandler, workloadGroupService);
    }

    /**
     * This class is mainly used to populate the workloadGroupId header
     * @param <T> T is Search related request
     */
    public static class RequestHandler<T extends TransportRequest> implements TransportRequestHandler<T> {

        private final ThreadPool threadPool;
        TransportRequestHandler<T> actualHandler;
        private final WorkloadGroupService workloadGroupService;

        public RequestHandler(ThreadPool threadPool, TransportRequestHandler<T> actualHandler, WorkloadGroupService workloadGroupService) {
            this.threadPool = threadPool;
            this.actualHandler = actualHandler;
            this.workloadGroupService = workloadGroupService;
        }

        @Override
        public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
            if (isSearchWorkloadRequest(task)) {
                ((WorkloadGroupTask) task).setWorkloadGroupId(threadPool.getThreadContext());
                final String workloadGroupId = ((WorkloadGroupTask) (task)).getWorkloadGroupId();
                workloadGroupService.rejectIfNeeded(workloadGroupId);
            }
            actualHandler.messageReceived(request, channel, task);
        }

        boolean isSearchWorkloadRequest(Task task) {
            return task instanceof WorkloadGroupTask;
        }
    }
}
