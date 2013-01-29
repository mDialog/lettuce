// Copyright (C) 2011 - Will Glozer.  All rights reserved.

package com.lambdaworks.redis;

import com.lambdaworks.codec.Base16;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.output.*;
import com.lambdaworks.redis.protocol.*;
import org.jboss.netty.channel.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import akka.dispatch.ExecutionContext;
import akka.dispatch.Promise;

import static com.lambdaworks.redis.protocol.CommandKeyword.*;
import static com.lambdaworks.redis.protocol.CommandType.*;

/**
 * An asynchronous thread-safe connection to a redis server. Multiple threads may
 * share one {@link RedisAsyncConnection} provided they avoid blocking and transactional
 * operations such as {@link #blpop} and {@link #multi()}/{@link #exec}.
 *
 * A {@link ConnectionWatchdog} monitors each connection and reconnects
 * automatically until {@link #close} is called. All pending commands will be
 * (re)sent after successful reconnection.
 *
 * @author Will Glozer
 */
public class RedisAsyncConnection<K, V> extends SimpleChannelUpstreamHandler {
    protected BlockingQueue<Command<K, V, ?>> queue;
    protected RedisCodec<K, V> codec;
    protected Channel channel;
    protected long timeout;
    protected TimeUnit unit;
    protected MultiOutput<K, V> multi;
    private String password;
    private int db;
    private boolean closed;
    private ExecutionContext executor;

    /**
     * Initialize a new connection.
     *
     * @param queue   Command queue.
     * @param codec   Codec used to encode/decode keys and values.
     * @param timeout Maximum time to wait for a response.
     * @param unit    Unit of time for the timeout.
     */
    public RedisAsyncConnection(BlockingQueue<Command<K, V, ?>> queue, RedisCodec<K, V> codec, long timeout, TimeUnit unit, ExecutionContext executor) {
        this.queue = queue;
        this.codec = codec;
        this.timeout = timeout;
        this.unit = unit;
        this.executor = executor;
    }

    /**
     * Set the command timeout for this connection.
     *
     * @param timeout Command timeout.
     * @param unit    Unit of time for the timeout.
     */
    public void setTimeout(long timeout, TimeUnit unit) {
        this.timeout = timeout;
        this.unit = unit;
    }

    public akka.dispatch.Future<Long> append(K key, V value) {
        return dispatch(APPEND, new IntegerOutput<K, V>(codec), key, value);
    }

    public String auth(String password) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(password);
        Command<K, V, String> cmd = dispatchCmd(AUTH, new StatusOutput<K, V>(codec), args);
        String status = await(cmd, timeout, unit);
        if ("OK".equals(status)) this.password = password;
        return status;
    }

    public akka.dispatch.Future<String> bgrewriteaof() {
        return dispatch(BGREWRITEAOF, new StatusOutput<K, V>(codec));
    }

    public akka.dispatch.Future<String> bgsave() {
        return dispatch(BGSAVE, new StatusOutput<K, V>(codec));
    }

    public akka.dispatch.Future<Long> bitcount(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        return dispatch(BITCOUNT, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> bitcount(K key, long start, long end) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(start).add(end);
        return dispatch(BITCOUNT, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> bitopAnd(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(AND).addKey(destination).addKeys(keys);
        return dispatch(BITOP, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> bitopNot(K destination, K source) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(NOT).addKey(destination).addKey(source);
        return dispatch(BITOP, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> bitopOr(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(OR).addKey(destination).addKeys(keys);
        return dispatch(BITOP, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> bitopXor(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(XOR).addKey(destination).addKeys(keys);
        return dispatch(BITOP, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<KeyValue<K, V>> blpop(long timeout, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys).add(timeout);
        return dispatch(BLPOP, new KeyValueOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<KeyValue<K, V>> brpop(long timeout, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys).add(timeout);
        return dispatch(BRPOP, new KeyValueOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<V> brpoplpush(long timeout, K source, K destination) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(source).addKey(destination).add(timeout);
        return dispatch(BRPOPLPUSH, new ValueOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> clientKill(String addr) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(KILL).add(addr);
        return dispatch(CLIENT, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> clientList() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(LIST);
        return dispatch(CLIENT, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<String>> configGet(String parameter) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(GET).add(parameter);
        return dispatch(CONFIG, new StringListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> configResetstat() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(RESETSTAT);
        return dispatch(CONFIG, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> configSet(String parameter, String value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(SET).add(parameter).add(value);
        return dispatch(CONFIG, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> dbsize() {
        return dispatch(DBSIZE, new IntegerOutput<K, V>(codec));
    }

    public akka.dispatch.Future<String> debugObject(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(OBJECT).addKey(key);
        return dispatch(DEBUG, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> decr(K key) {
        return dispatch(DECR, new IntegerOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<Long> decrby(K key, long amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(amount);
        return dispatch(DECRBY, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> del(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(DEL, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> discard() {
        multi = null;
        return dispatch(DISCARD, new StatusOutput<K, V>(codec));
    }

    public akka.dispatch.Future<byte[]> dump(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        return dispatch(DUMP, new ByteArrayOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<V> echo(V msg) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addValue(msg);
        return dispatch(ECHO, new ValueOutput<K, V>(codec), args);
    }

    public <T> akka.dispatch.Future<T> eval(V script, ScriptOutputType type, K[] keys, V... values) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addValue(script).add(keys.length).addKeys(keys).addValues(values);
        CommandOutput<K, V, T> output = newScriptOutput(codec, type);
        return dispatch(EVAL, output, args);
    }

    public <T> akka.dispatch.Future<T> evalsha(String digest, ScriptOutputType type, K[] keys, V... values) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(digest).add(keys.length).addKeys(keys).addValues(values);
        CommandOutput<K, V, T> output = newScriptOutput(codec, type);
        return dispatch(EVALSHA, output, args);
    }

    public akka.dispatch.Future<Boolean> exists(K key) {
        return dispatch(EXISTS, new BooleanOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<Boolean> expire(K key, long seconds) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(seconds);
        return dispatch(EXPIRE, new BooleanOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Boolean> expireat(K key, Date timestamp) {
        return expireat(key, timestamp.getTime() / 1000);
    }

    public akka.dispatch.Future<Boolean> expireat(K key, long timestamp) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(timestamp);
        return dispatch(EXPIREAT, new BooleanOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<Object>> exec() {
        MultiOutput<K, V> multi = this.multi;
        this.multi = null;
        return dispatch(EXEC, multi);
    }

    public akka.dispatch.Future<String> flushall() throws Exception {
        return dispatch(FLUSHALL, new StatusOutput<K, V>(codec));
    }

    public akka.dispatch.Future<String> flushdb() throws Exception {
        return dispatch(FLUSHDB, new StatusOutput<K, V>(codec));
    }

    public akka.dispatch.Future<V> get(K key) {
        return dispatch(GET, new ValueOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<Long> getbit(K key, long offset) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(offset);
        return dispatch(GETBIT, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<V> getrange(K key, long start, long end) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(end);
        return dispatch(GETRANGE, new ValueOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<V> getset(K key, V value) {
        return dispatch(GETSET, new ValueOutput<K, V>(codec), key, value);
    }

    public akka.dispatch.Future<Long> hdel(K key, K... fields) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKeys(fields);
        return dispatch(HDEL, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Boolean> hexists(K key, K field) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field);
        return dispatch(HEXISTS, new BooleanOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<V> hget(K key, K field) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field);
        return dispatch(HGET, new ValueOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> hincrby(K key, K field, long amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field).add(amount);
        return dispatch(HINCRBY, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Double> hincrbyfloat(K key, K field, double amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field).add(amount);
        return dispatch(HINCRBYFLOAT, new DoubleOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Map<K, V>> hgetall(K key) {
        return dispatch(HGETALL, new MapOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<List<K>> hkeys(K key) {
        return dispatch(HKEYS, new KeyListOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<Long> hlen(K key) {
        return dispatch(HLEN, new IntegerOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<List<V>> hmget(K key, K... fields) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKeys(fields);
        return dispatch(HMGET, new ValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> hmset(K key, Map<K, V> map) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(map);
        return dispatch(HMSET, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Boolean> hset(K key, K field, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field).addValue(value);
        return dispatch(HSET, new BooleanOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Boolean> hsetnx(K key, K field, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field).addValue(value);
        return dispatch(HSETNX, new BooleanOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<V>> hvals(K key) {
        return dispatch(HVALS, new ValueListOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<Long> incr(K key) {
        return dispatch(INCR, new IntegerOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<Long> incrby(K key, long amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(amount);
        return dispatch(INCRBY, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Double> incrbyfloat(K key, double amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(amount);
        return dispatch(INCRBYFLOAT, new DoubleOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> info() {
        return dispatch(INFO, new StatusOutput<K, V>(codec));
    }

    public akka.dispatch.Future<List<K>> keys(K pattern) {
       return dispatch(KEYS, new KeyListOutput<K, V>(codec), pattern);
    }

    public akka.dispatch.Future<Date> lastsave() {
        return dispatch(LASTSAVE, new DateOutput<K, V>(codec));
    }

    public akka.dispatch.Future<V> lindex(K key, long index) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(index);
        return dispatch(LINDEX, new ValueOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> linsert(K key, boolean before, V pivot, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(before ? BEFORE : AFTER).addValue(pivot).addValue(value);
        return dispatch(LINSERT, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> llen(K key) {
        return dispatch(LLEN, new IntegerOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<V> lpop(K key) {
        return dispatch(LPOP, new ValueOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<Long> lpush(K key, V... values) {
        return dispatch(LPUSH, new IntegerOutput<K, V>(codec), key, values);
    }

    public akka.dispatch.Future<Long> lpushx(K key, V value) {
        return dispatch(LPUSHX, new IntegerOutput<K, V>(codec), key, value);
    }

    public akka.dispatch.Future<List<V>> lrange(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(LRANGE, new ValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> lrem(K key, long count, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(count).addValue(value);
        return dispatch(LREM, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> lset(K key, long index, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(index).addValue(value);
        return dispatch(LSET, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> ltrim(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(LTRIM, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> migrate(String host, int port, K key, int db, long timeout) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(host).add(port).addKey(key).add(db).add(timeout);
        return dispatch(MIGRATE, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<V>> mget(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(MGET, new ValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Boolean> move(K key, int db) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(db);
        return dispatch(MOVE, new BooleanOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> multi() {
        Command<K, V, String> cmd = dispatchCmd(MULTI, new StatusOutput<K, V>(codec), null);
        multi = (multi == null ? new MultiOutput<K, V>(codec) : multi);
        return cmd.promise;
    }

    public akka.dispatch.Future<String> mset(Map<K, V> map) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(map);
        return dispatch(MSET, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Boolean> msetnx(Map<K, V> map) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(map);
        return dispatch(MSETNX, new BooleanOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> objectEncoding(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(ENCODING).addKey(key);
        return dispatch(OBJECT, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> objectIdletime(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(IDLETIME).addKey(key);
        return dispatch(OBJECT, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> objectRefcount(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(REFCOUNT).addKey(key);
        return dispatch(OBJECT, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Boolean> persist(K key) {
        return dispatch(PERSIST, new BooleanOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<Boolean> pexpire(K key, long milliseconds) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(milliseconds);
        return dispatch(PEXPIRE, new BooleanOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Boolean> pexpireat(K key, Date timestamp) {
        return pexpireat(key, timestamp.getTime());
    }

    public akka.dispatch.Future<Boolean> pexpireat(K key, long timestamp) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(timestamp);
        return dispatch(PEXPIREAT, new BooleanOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> ping() {
        return dispatch(PING, new StatusOutput<K, V>(codec));
    }

    public akka.dispatch.Future<Long> pttl(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        return dispatch(PTTL, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> publish(K channel, V message) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(channel).addValue(message);
        return dispatch(PUBLISH, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> quit() {
        return dispatch(QUIT, new StatusOutput<K, V>(codec));
    }

    public akka.dispatch.Future<V> randomkey() {
        return dispatch(RANDOMKEY, new ValueOutput<K, V>(codec));
    }

    public akka.dispatch.Future<String> rename(K key, K newKey) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(newKey);
        return dispatch(RENAME, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Boolean> renamenx(K key, K newKey) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(newKey);
        return dispatch(RENAMENX, new BooleanOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> restore(K key, long ttl, byte[] value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(ttl).add(value);
        return dispatch(RESTORE, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<V> rpop(K key) {
        return dispatch(RPOP, new ValueOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<V> rpoplpush(K source, K destination) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(source).addKey(destination);
        return dispatch(RPOPLPUSH, new ValueOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> rpush(K key, V... values) {
        return dispatch(RPUSH, new IntegerOutput<K, V>(codec), key, values);
    }

    public akka.dispatch.Future<Long> rpushx(K key, V value) {
        return dispatch(RPUSHX, new IntegerOutput<K, V>(codec), key, value);
    }

    public akka.dispatch.Future<Long> sadd(K key, V... members) {
        return dispatch(SADD, new IntegerOutput<K, V>(codec), key, members);
    }

    public akka.dispatch.Future<String> save() {
        return dispatch(SAVE, new StatusOutput<K, V>(codec));
    }

    public akka.dispatch.Future<Long> scard(K key) {
        return dispatch(SCARD, new IntegerOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<List<Boolean>> scriptExists(String... digests) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(EXISTS);
        for (String sha : digests) args.add(sha);
        return dispatch(SCRIPT, new BooleanListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> scriptFlush() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(FLUSH);
        return dispatch(SCRIPT, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> scriptKill() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(KILL);
        return dispatch(SCRIPT, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> scriptLoad(V script) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(LOAD).addValue(script);
        return dispatch(SCRIPT, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Set<V>> sdiff(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(SDIFF, new ValueSetOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> sdiffstore(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(destination).addKeys(keys);
        return dispatch(SDIFFSTORE, new IntegerOutput<K, V>(codec), args);
    }

    public String select(int db) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(db);
        Command<K, V, String> cmd = dispatchCmd(SELECT, new StatusOutput<K, V>(codec), args);
        String status = await(cmd, timeout, unit);
        if ("OK".equals(status)) this.db = db;
        return status;
    }

    public akka.dispatch.Future<String> set(K key, V value) {
        return dispatch(SET, new StatusOutput<K, V>(codec), key, value);
    }

    public akka.dispatch.Future<Long> setbit(K key, long offset, int value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(offset).add(value);
        return dispatch(SETBIT, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> setex(K key, long seconds, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(seconds).addValue(value);
        return dispatch(SETEX, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Boolean> setnx(K key, V value) {
        return dispatch(SETNX, new BooleanOutput<K, V>(codec), key, value);
    }

    public akka.dispatch.Future<Long> setrange(K key, long offset, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(offset).addValue(value);
        return dispatch(SETRANGE, new IntegerOutput<K, V>(codec), args);
    }

    @Deprecated
    public void shutdown() {
        dispatch(SHUTDOWN, new StatusOutput<K, V>(codec));
    }

    public void shutdown(boolean save) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        dispatch(SHUTDOWN, new StatusOutput<K, V>(codec), save ? args.add(SAVE) : args.add(NOSAVE));
    }

    public akka.dispatch.Future<Set<V>> sinter(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(SINTER, new ValueSetOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> sinterstore(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(destination).addKeys(keys);
        return dispatch(SINTERSTORE, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Boolean> sismember(K key, V member) {
        return dispatch(SISMEMBER, new BooleanOutput<K, V>(codec), key, member);
    }

    public akka.dispatch.Future<Boolean> smove(K source, K destination, V member) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(source).addKey(destination).addValue(member);
        return dispatch(SMOVE, new BooleanOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> slaveof(String host, int port) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(host).add(port);
        return dispatch(SLAVEOF, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> slaveofNoOne() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(NO).add(ONE);
        return dispatch(SLAVEOF, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<Object>> slowlogGet() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(GET);
        return dispatch(SLOWLOG, new NestedMultiOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<Object>> slowlogGet(int count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(GET).add(count);
        return dispatch(SLOWLOG, new NestedMultiOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> slowlogLen() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(LEN);
        return dispatch(SLOWLOG, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> slowlogReset() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(RESET);
        return dispatch(SLOWLOG, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Set<V>> smembers(K key) {
        return dispatch(SMEMBERS, new ValueSetOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<List<V>> sort(K key) {
        return dispatch(SORT, new ValueListOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<List<V>> sort(K key, SortArgs sortArgs) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        sortArgs.build(args, null);
        return dispatch(SORT, new ValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> sortStore(K key, SortArgs sortArgs, K destination) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        sortArgs.build(args, destination);
        return dispatch(SORT, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<V> spop(K key) {
        return dispatch(SPOP, new ValueOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<V> srandmember(K key) {
        return dispatch(SRANDMEMBER, new ValueOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<Set<V>> srandmember(K key, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(count);
        return dispatch(SRANDMEMBER, new ValueSetOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> srem(K key, V... members) {
        return dispatch(SREM, new IntegerOutput<K, V>(codec), key, members);
    }

    public akka.dispatch.Future<Set<V>> sunion(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(SUNION, new ValueSetOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> sunionstore(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(destination).addKeys(keys);
        return dispatch(SUNIONSTORE, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> sync() {
        return dispatch(SYNC, new StatusOutput<K, V>(codec));
    }

    public akka.dispatch.Future<Long> strlen(K key) {
        return dispatch(STRLEN, new IntegerOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<Long> ttl(K key) {
        return dispatch(TTL, new IntegerOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<String> type(K key) {
        return dispatch(TYPE, new StatusOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<String> watch(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(WATCH, new StatusOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<String> unwatch() {
        return dispatch(UNWATCH, new StatusOutput<K, V>(codec));
    }

    public akka.dispatch.Future<Long> zadd(K key, double score, V member) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(score).addValue(member);
        return dispatch(ZADD, new IntegerOutput<K, V>(codec), args);
    }

    @SuppressWarnings("unchecked")
    public akka.dispatch.Future<Long> zadd(K key, Object... scoresAndValues) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        for (int i = 0; i < scoresAndValues.length; i += 2) {
            args.add((Double) scoresAndValues[i]);
            args.addValue((V) scoresAndValues[i + 1]);
        }
        return dispatch(ZADD, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> zcard(K key) {
        return dispatch(ZCARD, new IntegerOutput<K, V>(codec), key);
    }

    public akka.dispatch.Future<Long> zcount(K key, double min, double max) {
        return zcount(key, string(min), string(max));
    }

    public akka.dispatch.Future<Long> zcount(K key, String min, String max) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(min).add(max);
        return dispatch(ZCOUNT, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Double> zincrby(K key, double amount, K member) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(amount).addKey(member);
        return dispatch(ZINCRBY, new DoubleOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> zinterstore(K destination, K... keys) {
        return zinterstore(destination, new ZStoreArgs(), keys);
    }

    public akka.dispatch.Future<Long> zinterstore(K destination, ZStoreArgs storeArgs, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(destination).add(keys.length).addKeys(keys);
        storeArgs.build(args);
        return dispatch(ZINTERSTORE, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<V>> zrange(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(ZRANGE, new ValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<ScoredValue<V>>> zrangeWithScores(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(start).add(stop).add(WITHSCORES);
        return dispatch(ZRANGE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<V>> zrangebyscore(K key, double min, double max) {
        return zrangebyscore(key, string(min), string(max));
    }

    public akka.dispatch.Future<List<V>> zrangebyscore(K key, String min, String max) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(min).add(max);
        return dispatch(ZRANGEBYSCORE, new ValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<V>> zrangebyscore(K key, double min, double max, long offset, long count) {
        return zrangebyscore(key, string(min), string(max), offset, count);
    }

    public akka.dispatch.Future<List<V>> zrangebyscore(K key, String min, String max, long offset, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(min).add(max).add(LIMIT).add(offset).add(count);
        return dispatch(ZRANGEBYSCORE, new ValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, double min, double max) {
        return zrangebyscoreWithScores(key, string(min), string(max));
    }

    public akka.dispatch.Future<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, String min, String max) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(min).add(max).add(WITHSCORES);
        return dispatch(ZRANGEBYSCORE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, double min, double max, long offset, long count) {
        return zrangebyscoreWithScores(key, string(min), string(max), offset, count);
    }

    public akka.dispatch.Future<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, String min, String max, long offset, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(min).add(max).add(WITHSCORES).add(LIMIT).add(offset).add(count);
        return dispatch(ZRANGEBYSCORE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> zrank(K key, V member) {
        return dispatch(ZRANK, new IntegerOutput<K, V>(codec), key, member);
    }

    public akka.dispatch.Future<Long> zrem(K key, V... members) {
        return dispatch(ZREM, new IntegerOutput<K, V>(codec), key, members);
    }

    public akka.dispatch.Future<Long> zremrangebyrank(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(ZREMRANGEBYRANK, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> zremrangebyscore(K key, double min, double max) {
        return zremrangebyscore(key, string(min), string(max));
    }

    public akka.dispatch.Future<Long> zremrangebyscore(K key, String min, String max) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(min).add(max);
        return dispatch(ZREMRANGEBYSCORE, new IntegerOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<V>> zrevrange(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(ZREVRANGE, new ValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<ScoredValue<V>>> zrevrangeWithScores(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(start).add(stop).add(WITHSCORES);
        return dispatch(ZREVRANGE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<V>> zrevrangebyscore(K key, double max, double min) {
        return zrevrangebyscore(key, string(max), string(min));
    }

    public akka.dispatch.Future<List<V>> zrevrangebyscore(K key, String max, String min) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(max).add(min);
        return dispatch(ZREVRANGEBYSCORE, new ValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<V>> zrevrangebyscore(K key, double max, double min, long offset, long count) {
        return zrevrangebyscore(key, string(max), string(min), offset, count);
    }

    public akka.dispatch.Future<List<V>> zrevrangebyscore(K key, String max, String min, long offset, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(max).add(min).add(LIMIT).add(offset).add(count);
        return dispatch(ZREVRANGEBYSCORE, new ValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<ScoredValue<V>>> zrevrangebyscoreWithScores(K key, double max, double min) {
        return zrevrangebyscoreWithScores(key, string(max), string(min));
    }

    public akka.dispatch.Future<List<ScoredValue<V>>> zrevrangebyscoreWithScores(K key, String max, String min) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(max).add(min).add(WITHSCORES);
        return dispatch(ZREVRANGEBYSCORE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<List<ScoredValue<V>>> zrevrangebyscoreWithScores(K key, double max, double min, long offset, long count) {
        return zrevrangebyscoreWithScores(key, string(max), string(min), offset, count);
    }

    public akka.dispatch.Future<List<ScoredValue<V>>> zrevrangebyscoreWithScores(K key, String max, String min, long offset, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(max).add(min).add(WITHSCORES).add(LIMIT).add(offset).add(count);
        return dispatch(ZREVRANGEBYSCORE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public akka.dispatch.Future<Long> zrevrank(K key, V member) {
        return dispatch(ZREVRANK, new IntegerOutput<K, V>(codec), key, member);
    }

    public akka.dispatch.Future<Double> zscore(K key, V member) {
        return dispatch(ZSCORE, new DoubleOutput<K, V>(codec), key, member);
    }

    public akka.dispatch.Future<Long> zunionstore(K destination, K... keys) {
        return zunionstore(destination, new ZStoreArgs(), keys);
    }

    public akka.dispatch.Future<Long> zunionstore(K destination, ZStoreArgs storeArgs, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(destination).add(keys.length).addKeys(keys);
        storeArgs.build(args);
        return dispatch(ZUNIONSTORE, new IntegerOutput<K, V>(codec), args);
    }

    /**
     * Wait until commands are complete or the connection timeout is reached.
     *
     * @param futures   Futures to wait for.
     *
     * @return True if all futures complete in time.
     */
    public boolean awaitAll(Future<?>... futures) {
        return awaitAll(timeout, unit, futures);
    }

    /**
     * Wait until futures are complete or the supplied timeout is reached.
     *
     * @param timeout   Maximum time to wait for futures to complete.
     * @param unit      Unit of time for the timeout.
     * @param futures   Futures to wait for.
     *
     * @return True if all futures complete in time.
     */
    public boolean awaitAll(long timeout, TimeUnit unit, Future<?>... futures) {
        boolean complete;

        try {
            long nanos = unit.toNanos(timeout);
            long time  = System.nanoTime();

            for (Future<?> f : futures) {
                if (nanos < 0) return false;
                f.get(nanos, TimeUnit.NANOSECONDS);
                long now = System.nanoTime();
                nanos -= now - time;
                time   = now;
            }

            complete = true;
        } catch (TimeoutException e) {
            complete = false;
        } catch (Exception e) {
            throw new RedisCommandInterruptedException(e);
        }

        return complete;
    }

    /**
     * Close the connection.
     */
    public synchronized void close() {
        if (!closed && channel != null) {
            ConnectionWatchdog watchdog = channel.getPipeline().get(ConnectionWatchdog.class);
            watchdog.setReconnect(false);
            closed = true;
            channel.close();
        }
    }

    public String digest(V script) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            md.update(codec.encodeValue(script));
            return new String(Base16.encode(md.digest(), false));
        } catch (NoSuchAlgorithmException e) {
            throw new RedisException("JVM does not support SHA1");
        }
    }

    @Override
    public synchronized void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channel = ctx.getChannel();

        List<Command<K, V, ?>> tmp = new ArrayList<Command<K, V, ?>>(queue.size() + 2);

        if (password != null) {
            CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(password);
            tmp.add(new Command<K, V, String>(AUTH, new StatusOutput<K, V>(codec), args, false, executor));
        }

        if (db != 0) {
            CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(db);
            tmp.add(new Command<K, V, String>(SELECT, new StatusOutput<K, V>(codec), args, false, executor));
        }

        tmp.addAll(queue);
        queue.clear();

        for (Command<K, V, ?> cmd : tmp) {
            if (!cmd.isCancelled()) {
                queue.add(cmd);
                channel.write(cmd);
            }
        }

        tmp.clear();
    }

    @Override
    public synchronized void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        if (closed) {
            for (Command<K, V, ?> cmd : queue) {
                cmd.getOutput().setError("Connection closed");
                cmd.complete();
            }
            queue.clear();
            queue = null;
            channel = null;
        }
    }

    public <T> akka.dispatch.Future<T> dispatch(CommandType type, CommandOutput<K, V, T> output) {
        return dispatch(type, output, (CommandArgs<K, V>) null);
    }

    public <T> akka.dispatch.Future<T> dispatch(CommandType type, CommandOutput<K, V, T> output, K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        return dispatch(type, output, args);
    }

    public <T> akka.dispatch.Future<T> dispatch(CommandType type, CommandOutput<K, V, T> output, K key, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addValue(value);
        return dispatch(type, output, args);
    }

    public <T> akka.dispatch.Future<T> dispatch(CommandType type, CommandOutput<K, V, T> output, K key, V[] values) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addValues(values);
        return dispatch(type, output, args);
    }

    public <T> akka.dispatch.Future<T> dispatch(CommandType type, CommandOutput<K, V, T> output, CommandArgs<K, V> args) {
        return dispatchCmd(type, output, args).promise;
    }

    public synchronized <T> Command<K, V, T> dispatchCmd(CommandType type, CommandOutput<K, V, T> output, CommandArgs<K, V> args) {
            Command<K, V, T> cmd = new Command<K, V, T>(type, output, args, multi != null, executor);

        try {
            if (multi != null) {
                multi.add(cmd);
            }

            queue.put(cmd);

            if (channel != null) {
                channel.write(cmd);
            }
        } catch (NullPointerException e) {
            throw new RedisException("Connection is closed");
        } catch (InterruptedException e) {
            throw new RedisCommandInterruptedException(e);
        }

        return cmd;
    }

    public <T> T await(Command<K, V, T> cmd, long timeout, TimeUnit unit) {
        if (!cmd.await(timeout, unit)) {
            cmd.cancel(true);
            throw new RedisException("Command timed out");
        }
        CommandOutput<K, V, T> output = cmd.getOutput();
        if (output.hasError()) throw new RedisException(output.getError());
        return output.get();
    }

    @SuppressWarnings("unchecked")
    protected <K, V, T> CommandOutput<K, V, T> newScriptOutput(RedisCodec<K, V> codec, ScriptOutputType type) {
        switch (type) {
            case BOOLEAN: return (CommandOutput<K, V, T>) new BooleanOutput<K, V>(codec);
            case INTEGER: return (CommandOutput<K, V, T>) new IntegerOutput<K, V>(codec);
            case STATUS:  return (CommandOutput<K, V, T>) new StatusOutput<K, V>(codec);
            case MULTI:   return (CommandOutput<K, V, T>) new NestedMultiOutput<K, V>(codec);
            case VALUE:   return (CommandOutput<K, V, T>) new ValueOutput<K, V>(codec);
            default:      throw new RedisException("Unsupported script output type");
        }
    }

    public String string(double n) {
        if (Double.isInfinite(n)) {
            return (n > 0) ? "+inf" : "-inf";
        }
        return Double.toString(n);
    }
}
