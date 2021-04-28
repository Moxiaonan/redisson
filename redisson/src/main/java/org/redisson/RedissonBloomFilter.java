/**
 * Copyright (c) 2013-2021 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.redisson;

import io.netty.buffer.ByteBuf;
import org.redisson.api.RBitSetAsync;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RFuture;
import org.redisson.client.RedisException;
import org.redisson.client.codec.*;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.convertor.VoidReplayConvertor;
import org.redisson.client.protocol.decoder.ObjectMapReplayDecoder;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandBatchService;
import org.redisson.misc.Hash;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Bloom filter based on Highway 128-bit hash.
 *
 * @author Nikita Koksharov
 *
 * @param <T> type of object
 */
public class RedissonBloomFilter<T> extends RedissonExpirable implements RBloomFilter<T> {

    private volatile long size;
    private volatile int hashIterations;

    private final CommandAsyncExecutor commandExecutor;
    private String configName;

    protected RedissonBloomFilter(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.configName = suffixName(getRawName(), "config");
    }

    protected RedissonBloomFilter(Codec codec, CommandAsyncExecutor commandExecutor, String name) {
        super(codec, commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.configName = suffixName(getRawName(), "config");
    }

    private int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
      }

    private long optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }
    
    private long[] hash(Object object) {
        // 将对象转为 ByteBuf
        ByteBuf state = encode(object);
        try {
            // 获取 object 的 hash 值
            return Hash.hash128(state);
        } finally {
            state.release();
        }
    }

    @Override
    public boolean add(T object) {
        // 对象转为 hash 这里是 size == 2 的long数组
        long[] hashes = hash(object);

        while (true) {
            if (size == 0) {
                readConfig();
            }

            // 获取 hash 函数个数
            int hashIterations = this.hashIterations;
            // 获取 hash槽的 size
            long size = this.size;

            // 计算这个 hash 值在Redis bitmap（String）的下标
            long[] indexes = hash(hashes[0], hashes[1], hashIterations, size);

            CommandBatchService executorService = new CommandBatchService(commandExecutor);
            // 检查 config 信息
            addConfigCheck(hashIterations, size, executorService);
            RBitSetAsync bs = createBitSet(executorService);
            // 循环上一步获得的下标 , 将 Redis bitmap（String）对应位置执行 SETBIT 指令
            for (int i = 0; i < indexes.length; i++) {
                bs.setAsync(indexes[i]);
            }
            try {
                List<Boolean> result = (List<Boolean>) executorService.execute().getResponses();

                // 结果集合 , 结果集合中 , 第一个是检查config信息的结果 , 所以跳过 , 取下标为1开始的结果集
                for (Boolean val : result.subList(1, result.size()-1)) {
                    // SETBIT 指令会返回 原下标上的值 如果是 0(false) 表示 原位置上没有为1 直接返回true
                    if (!val) {
                        return true;
                    }
                }
                // 如果 SETBIT 指令返回的值都是1 则说明所有下标都已被设置为 1 返回false
                return false;
            } catch (RedisException e) {
                if (e.getMessage() == null || !e.getMessage().contains("Bloom filter config has been changed")) {
                    throw e;
                }
            }
        }
    }

    private long[] hash(long hash1, long hash2, int iterations, long size) {
        long[] indexes = new long[iterations];
        long hash = hash1;
        // 根据 iterations(hash函数的个数) 循环计算出 iterations(hash函数的个数) 个 bitmap 下标
        for (int i = 0; i < iterations; i++) {
            // 这个 & 操作是取 hash 的绝对值
            // 对 size 取余 计算 bitmap 范围内的下标
            indexes[i] = (hash & Long.MAX_VALUE) % size;
            // 根据hash1 hash2累加生成一个新的hash值 用于下次计算
            // 这里不采用大部分资料里说的 使用n个hash函数 而是直接用一个hash 生成了n个hash值
            if (i % 2 == 0) {
                hash += hash2;
            } else {
                hash += hash1;
            }
        }
        // 返回 iterations(hash函数的个数) 个下标
        return indexes;
    }

    @Override
    public boolean contains(T object) {
        // 对象转为 hash 值
        long[] hashes = hash(object);

        while (true) {
            if (size == 0) {
                readConfig();
            }

            int hashIterations = this.hashIterations;
            long size = this.size;

            // 计算这个 hash 值在Redis bitmap（String）的下标
            long[] indexes = hash(hashes[0], hashes[1], hashIterations, size);

            CommandBatchService executorService = new CommandBatchService(commandExecutor);
            addConfigCheck(hashIterations, size, executorService);
            RBitSetAsync bs = createBitSet(executorService);

            // 循环上一步获得的下标 , 将 Redis bitmap（String）对应位置执行 GETBIT 操作
            for (int i = 0; i < indexes.length; i++) {
                bs.getAsync(indexes[i]);
            }
            try {
                List<Boolean> result = (List<Boolean>) executorService.execute().getResponses();

                for (Boolean val : result.subList(1, result.size()-1)) {
                    if (!val) {
                        return false;
                    }
                }

                return true;
            } catch (RedisException e) {
                if (e.getMessage() == null || !e.getMessage().contains("Bloom filter config has been changed")) {
                    throw e;
                }
            }
        }
    }

    protected RBitSetAsync createBitSet(CommandBatchService executorService) {
        return new RedissonBitSet(executorService, getRawName());
    }

    private void addConfigCheck(int hashIterations, long size, CommandBatchService executorService) {
        executorService.evalReadAsync(configName, codec, RedisCommands.EVAL_VOID,
                "local size = redis.call('hget', KEYS[1], 'size');" +
                        "local hashIterations = redis.call('hget', KEYS[1], 'hashIterations');" +
                        "assert(size == ARGV[1] and hashIterations == ARGV[2], 'Bloom filter config has been changed')",
                        Arrays.<Object>asList(configName), size, hashIterations);
    }

    @Override
    public long count() {
        CommandBatchService executorService = new CommandBatchService(commandExecutor);
        RFuture<Map<String, String>> configFuture = executorService.readAsync(configName, StringCodec.INSTANCE,
                new RedisCommand<Map<Object, Object>>("HGETALL", new ObjectMapReplayDecoder()), configName);
        RBitSetAsync bs = createBitSet(executorService);
        RFuture<Long> cardinalityFuture = bs.cardinalityAsync();
        executorService.execute();

        readConfig(configFuture.getNow());

        return Math.round(-size / ((double) hashIterations) * Math.log(1 - cardinalityFuture.getNow() / ((double) size)));
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return deleteAsync(getRawName(), configName);
    }

    @Override
    public RFuture<Long> sizeInMemoryAsync() {
        List<Object> keys = Arrays.<Object>asList(getRawName(), configName);
        return super.sizeInMemoryAsync(keys);
    }
    
    private void readConfig() {
        RFuture<Map<String, String>> future = commandExecutor.readAsync(configName, StringCodec.INSTANCE,
                new RedisCommand<Map<Object, Object>>("HGETALL", new ObjectMapReplayDecoder()), configName);
        Map<String, String> config = commandExecutor.get(future);

        readConfig(config);
    }

    private void readConfig(Map<String, String> config) {
        if (config.get("hashIterations") == null
                || config.get("size") == null) {
            throw new IllegalStateException("Bloom filter is not initialized!");
        }
        size = Long.valueOf(config.get("size"));
        hashIterations = Integer.valueOf(config.get("hashIterations"));
    }

    protected long getMaxSize() {
        return Integer.MAX_VALUE*2L;
    }
    
    @Override
    public boolean tryInit(long expectedInsertions, double falseProbability) {
        if (falseProbability > 1) {
            throw new IllegalArgumentException("Bloom filter false probability can't be greater than 1");
        }
        if (falseProbability < 0) {
            throw new IllegalArgumentException("Bloom filter false probability can't be negative");
        }

        // 计算 bitmap 长度
        size = optimalNumOfBits(expectedInsertions, falseProbability);
        if (size == 0) {
            throw new IllegalArgumentException("Bloom filter calculated size is " + size);
        }
        if (size > getMaxSize()) {
            throw new IllegalArgumentException("Bloom filter size can't be greater than " + getMaxSize() + ". But calculated size is " + size);
        }
        // 计算 hash 函数个数
        hashIterations = optimalNumOfHashFunctions(expectedInsertions, size);

        CommandBatchService executorService = new CommandBatchService(commandExecutor);
        // 先获取原有信息 , 如果有配置信息 , 则判断与当前参数是否匹配
        executorService.evalReadAsync(configName, codec, RedisCommands.EVAL_VOID,
                "local size = redis.call('hget', KEYS[1], 'size');" +
                        "local hashIterations = redis.call('hget', KEYS[1], 'hashIterations');" +
                        "assert(size == false and hashIterations == false, 'Bloom filter config has been changed')",
                        Arrays.<Object>asList(configName), size, hashIterations);
        // 初始化 {config} 信息 (即使上一步中信息不匹配 , 仍会将新的信息设置到 {config} 中)
        executorService.writeAsync(configName, StringCodec.INSTANCE,
                                                new RedisCommand<Void>("HMSET", new VoidReplayConvertor()), configName,
                "size", size, "hashIterations", hashIterations,
                "expectedInsertions", expectedInsertions, "falseProbability", BigDecimal.valueOf(falseProbability).toPlainString());
        try {
            executorService.execute();
        } catch (RedisException e) {
            if (e.getMessage() == null || !e.getMessage().contains("Bloom filter config has been changed")) {
                throw e;
            }
            readConfig();
            // 如果信息不匹配,则返回false,但仍会将新的信息设置到 {config} 中
            return false;
        }

        return true;
    }

    @Override
    public RFuture<Boolean> expireAsync(long timeToLive, TimeUnit timeUnit) {
        return expireAsync(timeToLive, timeUnit, getRawName(), configName);
    }

    @Override
    public RFuture<Boolean> expireAtAsync(long timestamp) {
        return expireAtAsync(timestamp, getRawName(), configName);
    }

    @Override
    public RFuture<Boolean> clearExpireAsync() {
        return clearExpireAsync(getRawName(), configName);
    }
    
    @Override
    public long getExpectedInsertions() {
        Long result = get(commandExecutor.readAsync(configName, LongCodec.INSTANCE, RedisCommands.HGET, configName, "expectedInsertions"));
        return check(result);
    }

    @Override
    public double getFalseProbability() {
        Double result = get(commandExecutor.readAsync(configName, DoubleCodec.INSTANCE, RedisCommands.HGET, configName, "falseProbability"));
        return check(result);
    }

    @Override
    public long getSize() {
        Long result = get(commandExecutor.readAsync(configName, LongCodec.INSTANCE, RedisCommands.HGET, configName, "size"));
        return check(result);
    }

    @Override
    public int getHashIterations() {
        Integer result = get(commandExecutor.readAsync(configName, IntegerCodec.INSTANCE, RedisCommands.HGET, configName, "hashIterations"));
        return check(result);
    }

    @Override
    public RFuture<Boolean> isExistsAsync() {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.EXISTS, getRawName(), configName);
    }

    @Override
    public RFuture<Void> renameAsync(String newName) {
        String newConfigName = suffixName(newName, "config");
        RFuture<Void> f = commandExecutor.evalWriteAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.EVAL_VOID,
                     "if redis.call('exists', KEYS[1]) == 1 then " +
                              "redis.call('rename', KEYS[1], ARGV[1]); " +
                          "end; " +
                          "return redis.call('rename', KEYS[2], ARGV[2]); ",
                Arrays.<Object>asList(getRawName(), configName), newName, newConfigName);
        f.onComplete((value, e) -> {
            if (e == null) {
                setName(newName);
                this.configName = newConfigName;
            }
        });
        return f;
    }

    @Override
    public RFuture<Boolean> renamenxAsync(String newName) {
        String newConfigName = suffixName(newName, "config");
        RFuture<Boolean> f = commandExecutor.evalWriteAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local r = redis.call('renamenx', KEYS[1], ARGV[1]); "
                        + "if r == 0 then "
                        + "  return 0; "
                        + "else  "
                        + "  return redis.call('renamenx', KEYS[2], ARGV[2]); "
                        + "end; ",
                Arrays.<Object>asList(getRawName(), configName), newName, newConfigName);
        f.onComplete((value, e) -> {
            if (e == null && value) {
                setName(newName);
                this.configName = newConfigName;
            }
        });
        return f;
    }

    private <V> V check(V result) {
        if (result == null) {
            throw new IllegalStateException("Bloom filter is not initialized!");
        }
        return result;
    }

}
