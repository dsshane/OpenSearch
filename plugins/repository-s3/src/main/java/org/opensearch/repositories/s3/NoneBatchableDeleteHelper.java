/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
package org.opensearch.repositories.s3;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NoneBatchableDeleteHelper {
    public static int MAX_DELETE_THREADS = 50;

    public static class DefaultThreadFactory implements ThreadFactory {

        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        public DefaultThreadFactory() {
            this("pool-" + poolNumber.getAndIncrement() + "-thread-");
        }

        public DefaultThreadFactory(String namePrefix) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = namePrefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (t.isDaemon()) t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }

    }

    /**
     * Deletes objects from the S3 bucket using the provided S3AsyncClient and an iterator of ListObjectsV2Response.
     * @param s3AsyncClient
     * @param bucketName
     * @param listObjectsResponseIterator
     * @throws SdkException
     */
    public static void deleteObjectsFromListObjectResultIgnoreNotExists(
        S3AsyncClient s3AsyncClient,
        String bucketName,
        Iterator<ListObjectsV2Response> listObjectsResponseIterator
    ) throws SdkException {

        DefaultThreadFactory threadFactory = new DefaultThreadFactory("s3-delete-objects-");
        final ExecutorService deleteTaskPool = Executors.newFixedThreadPool(MAX_DELETE_THREADS, threadFactory);

        try {
            while (listObjectsResponseIterator.hasNext()) {
                ListObjectsV2Response listObjectsResponse = SocketAccess.doPrivileged(listObjectsResponseIterator::next);
                if (listObjectsResponse.hasContents()) {
                    listObjectsResponse.contents().forEach(s3Object -> {
                        deleteTaskPool.execute(() -> {
                            try {
                                s3AsyncClient.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build())
                                    .get();
                            } catch (Exception e) {
                                // Do nothing
                            }
                        });
                    });
                }
            }
        } finally {
            try {
                if (deleteTaskPool != null) {
                    deleteTaskPool.shutdown();
                    try {
                        deleteTaskPool.awaitTermination(60, TimeUnit.MINUTES);
                    } catch (Exception e) {}
                }
            } finally {
                if (deleteTaskPool != null) {
                    deleteTaskPool.shutdownNow();
                }
            }
        }
    }

    /**
     * Deletes objects from the S3 bucket using the provided S3AsyncClient and a list of object keys.
     * @param s3Client
     * @param bucketName
     * @param listObjects
     * @return Set of object keys that were not found or not deleted.
     * @throws SdkException
     */
    public static Set<String> deleteObjectsIgnoreNotExists(
        S3Client s3Client,
        String bucketName,
        List<String> listObjects,
        Set<String> outstanding
    ) throws SdkException {

        DefaultThreadFactory threadFactory = new DefaultThreadFactory("s3-delete-objects-");
        final ExecutorService deleteTaskPool = Executors.newFixedThreadPool(MAX_DELETE_THREADS, threadFactory);
        Set<String> synchedOutstanding = Collections.synchronizedSet(outstanding);

        try {
            for (String objectKey : listObjects) {
                deleteTaskPool.execute(() -> {
                    try {
                        SocketAccess.doPrivilegedVoid(
                            () -> { s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(objectKey).build()); }
                        );
                        synchedOutstanding.remove(objectKey);
                    } catch (S3Exception e) {
                        if ("NoSuchKey".equalsIgnoreCase(e.awsErrorDetails().errorCode())) {
                            synchedOutstanding.remove(objectKey);
                        }
                        // Else, do nothing, just keep the key as outstanding.
                    } catch (Exception e) {
                        String errorMessage = e.getMessage();
                        // Do nothing, just keep the key as outstanding.
                    }
                });
            }
        } finally {
            try {
                if (deleteTaskPool != null) {
                    deleteTaskPool.shutdown();
                    try {
                        deleteTaskPool.awaitTermination(60, TimeUnit.MINUTES);
                    } catch (Exception e) {}
                }
            } finally {
                if (deleteTaskPool != null) {
                    deleteTaskPool.shutdownNow();
                }
            }
        }
        return synchedOutstanding;
    }

    /**
     * Deletes objects from the S3 bucket using the provided S3AsyncClient and an iterator of ListObjectsV2Response.
     * @param s3Client
     * @param bucketName
     * @param listObjectsResponseIterator
     * @throws SdkException
     */
    public static void deleteObjectsFromListObjectResultIgnoreNotExists(
        S3Client s3Client,
        String bucketName,
        Iterator<ListObjectsV2Response> listObjectsResponseIterator
    ) throws SdkException {

        DefaultThreadFactory threadFactory = new DefaultThreadFactory("s3-delete-objects-");
        final ExecutorService deleteTaskPool = Executors.newFixedThreadPool(MAX_DELETE_THREADS, threadFactory);

        try {
            while (listObjectsResponseIterator.hasNext()) {
                ListObjectsV2Response listObjectsResponse = SocketAccess.doPrivileged(listObjectsResponseIterator::next);
                if (listObjectsResponse.hasContents()) {
                    listObjectsResponse.contents().forEach(s3Object -> {
                        deleteTaskPool.execute(() -> {
                            try {
                                s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build());
                            } catch (Exception e) {
                                // Do nothing
                            }
                        });
                    });
                }
            }
        } finally {
            try {
                if (deleteTaskPool != null) {
                    deleteTaskPool.shutdown();
                    try {
                        deleteTaskPool.awaitTermination(60, TimeUnit.MINUTES);
                    } catch (Exception e) {}
                }
            } finally {
                if (deleteTaskPool != null) {
                    deleteTaskPool.shutdownNow();
                }
            }
        }
    }

    /**
     * Deletes objects from the S3 bucket using the provided S3AsyncClient and a list of object keys.
     * @param s3AsyncClient
     * @param bucketName
     * @param listObjects
     * @throws SdkException
     */
    public static void deleteObjectsIgnoreNotExists(S3AsyncClient s3AsyncClient, String bucketName, List<String> listObjects)
        throws SdkException {

        DefaultThreadFactory threadFactory = new DefaultThreadFactory("s3-delete-objects-");
        final ExecutorService deleteTaskPool = Executors.newFixedThreadPool(MAX_DELETE_THREADS, threadFactory);

        try {
            for (String objectKey : listObjects) {
                deleteTaskPool.execute(() -> {
                    try {
                        s3AsyncClient.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(objectKey).build()).get();
                    } catch (Exception e) {
                        // Do nothing
                    }
                });
            }
        } finally {
            try {
                if (deleteTaskPool != null) {
                    deleteTaskPool.shutdown();
                    try {
                        deleteTaskPool.awaitTermination(60, TimeUnit.MINUTES);
                    } catch (Exception e) {}
                }
            } finally {
                if (deleteTaskPool != null) {
                    deleteTaskPool.shutdownNow();
                }
            }
        }
    }
}
