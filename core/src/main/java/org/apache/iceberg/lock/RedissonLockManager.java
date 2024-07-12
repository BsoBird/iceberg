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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LockManagers;

import org.apache.iceberg.util.Tasks;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedissonLockManager extends LockManagers.BaseLockManager{

    public final static String REDISSON_LOCK_CONF = "lock.redisson.conf.yml.base64";
    public final static String REDISSON_LOCK_URL = "lock.redisson.conf.url";
    public final static String REDISSON_LOCK_KEY_PREFIX = "lock.redisson.key.prefix";
    public final static String REDISSON_LOCK_RETRY_MAX_TIMES = "lock.redisson.retry.max-times";
    public final static String REDISSON_LOCK_RETRY_MAX_TIMES_DEFAULT = "3";
    private static final Logger LOG = LoggerFactory.getLogger(RedissonLockManager.class);

    private final Map<String,RLock> lockCache = Maps.newHashMap();
    private RedissonClient redissonClient;
    private String prefix = null;
    private int maxRetryTimes = 10;
    public RedissonLockManager(){}

    public RedissonLockManager(String redisUrl){
        try {
            init(redisUrl,null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public RedissonLockManager(String redisUrl,String confYmlBase64){
        try {
            init(redisUrl,confYmlBase64);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void init(String redissonUrl,String redissonConfYmlBase64) throws IOException {
        if(redissonClient!=null){
            return;
        }
        if(!Strings.isNullOrEmpty(redissonConfYmlBase64)){
            String ymlConfig = new String(Base64.getDecoder().decode(redissonConfYmlBase64), StandardCharsets.UTF_8);
            Config config = Config.fromYAML(ymlConfig);
            redissonClient = Redisson.create(config);
        }else if(!Strings.isNullOrEmpty(redissonUrl)){
            Config config = new Config();
            config.useSingleServer().setAddress(redissonUrl);
            redissonClient = Redisson.create(config);
        }else{
            String msg = String.format("[%s] or [%s] must be set.",REDISSON_LOCK_CONF,REDISSON_LOCK_URL);
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public void initialize(Map<String, String> properties) {
        try{
            super.initialize(properties);
            String redissonConfYmlBase64 = properties.getOrDefault(REDISSON_LOCK_CONF,null);
            String redissonUrl = properties.getOrDefault(REDISSON_LOCK_URL,null);
            init(redissonUrl,redissonConfYmlBase64);
            this.prefix = properties.getOrDefault(REDISSON_LOCK_KEY_PREFIX,null);
            this.maxRetryTimes = Integer.parseInt(properties.getOrDefault(REDISSON_LOCK_RETRY_MAX_TIMES,REDISSON_LOCK_RETRY_MAX_TIMES_DEFAULT));
        }catch (Exception e){
            LOG.error("Unable to initialize redisson client.",e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (RLock lock : lockCache.values()) {
            if(lock.isLocked()){
                lock.unlock();
            }
        }
        redissonClient.shutdown();
    }

    @Override
    public boolean acquire(String entityId, String ownerId) {
        synchronized (lockCache){
            try{
                if(lockCache.get(ownerId)!=null){
                    return false;
                }
                Tasks.foreach(entityId)
                        .retry(maxRetryTimes - 1)
                        .onlyRetryOn(BadRequestException.class)
                        .throwFailureWhenFinished()
                        .exponentialBackoff(acquireIntervalMs(), acquireIntervalMs(), acquireTimeoutMs(), 1)
                        .run(id -> acquireOnce(id, ownerId));
                return true;
            }catch (Exception e){
                LOG.error("An exception occurred during the locking process.",e);
                return false;
            }
        }
    }

    private void acquireOnce(String entityId, String ownerId){
        try{
            String lockKey = prefix!=null?prefix+entityId:entityId;
            RLock rlock = redissonClient.getLock(lockKey);
            boolean success = rlock.tryLock(acquireTimeoutMs(),heartbeatTimeoutMs(), TimeUnit.MICROSECONDS);
            if(!success){
                throw new IllegalStateException("Unable to acquire lock " + entityId);
            }
            lockCache.putIfAbsent(ownerId,rlock);
        }catch (InterruptedException e){
            throw new BadRequestException(e,"connection refused.");
        }
    }

    @Override
    public boolean release(String entityId, String ownerId) {
        synchronized (lockCache){
            try{
                RLock rlock = lockCache.get(ownerId);
                if(rlock == null || !rlock.isLocked()){
                    return true;
                }
                rlock.unlock();
                lockCache.remove(ownerId);
                return true;
            }catch (Exception e){
                LOG.error("An exception occurred during the unlocking process.",e);
                return false;
            }
        }
    }
}
