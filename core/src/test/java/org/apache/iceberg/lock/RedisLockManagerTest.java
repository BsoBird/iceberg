package org.apache.iceberg.lock;


import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.github.fppt.jedismock.RedisServer;

import org.assertj.core.api.Assertions;


public class RedisLockManagerTest {
    private RedisServer redisServer = null;
    @Before
    public void before() throws IOException {
        redisServer = new RedisServer(6379);
        redisServer.start();
    }

    @After
    public void after() throws IOException {
        if(redisServer.isRunning()){
            redisServer.stop();
        }
    }

    @Test
    public void testRedissonLockManager(){
       RedissonLockManager redissonLockManager = new RedissonLockManager("redis://localhost:6379");
       String ownerId = UUID.randomUUID().toString();
       String entityId = UUID.randomUUID().toString();
       Assertions.assertThat(redissonLockManager.acquire(entityId,ownerId)).isTrue();
       Assertions.assertThat(redissonLockManager.release(entityId,ownerId)).isTrue();
    }

    @Test
    public void testConcurrentGetLock() throws Exception {
        String ownerId = UUID.randomUUID().toString();
        String entityId = UUID.randomUUID().toString();
        RedissonLockManager redissonLockManager = new RedissonLockManager("redis://localhost:6379");
        RedissonLockManager redissonLockManager2 = new RedissonLockManager("redis://localhost:6379");
        Map<String,String> conf = ImmutableMap.of(
                CatalogProperties.LOCK_ACQUIRE_TIMEOUT_MS, "10",
                CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS, "1000",
                CatalogProperties.LOCK_HEARTBEAT_TIMEOUT_MS, "1000000",
                RedissonLockManager.REDISSON_LOCK_RETRY_MAX_TIMES_DEFAULT,"1");
        redissonLockManager.initialize(conf);
        redissonLockManager2.initialize(conf);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
        AtomicInteger successCount = new AtomicInteger(0);
        executorService.execute(()->{
            try {
                countDownLatch.countDown();
                countDownLatch.await();
                boolean lockSuccess = redissonLockManager.acquire(entityId,ownerId);
                System.out.println(String.format("Thread-Id[%s],LockResult[%s]",Thread.currentThread().toString(),lockSuccess));
                if(lockSuccess){
                    successCount.incrementAndGet();
                }else{
                    successCount.decrementAndGet();
                }
            } catch (Exception e) {
                e.printStackTrace();
                unexpectedException.set(e);
            }
        });
        executorService.execute(()->{
            try {
                countDownLatch.countDown();
                countDownLatch.await();
                boolean lockSuccess = redissonLockManager2.acquire(entityId,ownerId);
                System.out.println(String.format("Thread-Id[%s],LockResult[%s]",Thread.currentThread().toString(),lockSuccess));
                if(lockSuccess){
                    successCount.incrementAndGet();
                }else{
                    successCount.decrementAndGet();
                }
            } catch (Exception e) {
                e.printStackTrace();
                unexpectedException.set(e);
            }
        });
        executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.SECONDS);
        redissonLockManager.release(entityId,ownerId);
        redissonLockManager2.release(entityId,ownerId);
        Assertions.assertThat(unexpectedException.get()).isNull();
        Assertions.assertThat(successCount.get()).isEqualTo(0);
    }

}