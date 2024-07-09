/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.lock;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import java.util.concurrent.TimeUnit;
import org.apache.curator.RetryPolicy;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LockManagers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperLockManager extends LockManagers.BaseLockManager{
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperLockManager.class);
    private CuratorFramework client;
    private final Map<String, InterProcessMutex> lockCache = Maps.newHashMap();
    private String rootNode = null;
    public ZookeeperLockManager(){}

    public ZookeeperLockManager(String zkIpPort,String rootNode){
        try{
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 30);
            client = CuratorFrameworkFactory.builder()
                    .connectString("192.168.128.129:2181")
                    .sessionTimeoutMs(5000)  // 会话超时时间
                    .connectionTimeoutMs(5000) // 连接超时时间
                    .retryPolicy(retryPolicy)
                    .namespace(rootNode) // 包含隔离名称(初始目录节点?)
                    .build();
            client.start();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initialize(Map<String, String> properties) {
        super.initialize(properties);
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (InterProcessMutex lock : lockCache.values()) {
            lock.release();
        }
        client.close();
    }

    @Override
    public boolean acquire(String entityId, String ownerId) {
        synchronized (lockCache){
            if(lockCache.get(ownerId)!=null){
                return false;
            }

            String lockId = new String(Base64.getUrlEncoder().encode(entityId.getBytes(StandardCharsets.UTF_8)));
            String path = "/"+rootNode+"/"+lockId;
            InterProcessMutex mutex = new InterProcessMutex(client, path);
            try {
                boolean lockSuccess = mutex.acquire(acquireTimeoutMs(), TimeUnit.MILLISECONDS);
                if(lockSuccess){
                    lockCache.putIfAbsent(ownerId,mutex);
                }
                return lockSuccess;
            } catch (Exception e) {
                LOG.error("An exception occurred during the locking process.",e);
            }
        }
        return false;
    }

    @Override
    public boolean release(String entityId, String ownerId) {
        try{
            InterProcessMutex lock = lockCache.get(ownerId);
            if(lock!=null && lock.isAcquiredInThisProcess()){
                lock.release();
            }
            return true;
        }catch (Exception e){
            LOG.error("An exception occurred during the locking process.",e);
        }
        return false;
    }
}
