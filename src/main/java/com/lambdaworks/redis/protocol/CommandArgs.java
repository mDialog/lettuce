// Copyright (C) 2011 - Will Glozer.  All rights reserved.

package com.lambdaworks.redis.protocol;

import com.lambdaworks.redis.codec.RedisCodec;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Map;

import static java.lang.Math.max;

/**
 * Redis command argument encoder.
 *
 * @author Will Glozer
 */
public class CommandArgs<K, V> {
    private static final byte[] CRLF = "\r\n".getBytes(Charsets.ASCII);

    private RedisCodec<K, V> codec;
    private ByteBuffer buffer;
    private int count;

    public CommandArgs(RedisCodec<K, V> codec) {
        this.codec  = codec;
        this.buffer = ByteBuffer.allocate(1024);
    }

    public ByteBuffer buffer() {
        buffer.flip();
        return buffer;
    }

    public int count() {
        return count;
    }

    public CommandArgs<K, V> addKey(K key) {
        return write(codec.encodeKey(key));
    }

    public CommandArgs<K, V> addKeys(K... keys) {
        for (K key : keys) {
            addKey(key);
        }
        return this;
    }

    public CommandArgs<K, V> addValue(V value) {
        return write(codec.encodeValue(value));
    }

    public CommandArgs<K, V> addValues(V... values) {
        for (V value : values) {
            addValue(value);
        }
        return this;
    }

    public CommandArgs<K, V> add(Map<K, V> map) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            write(codec.encodeKey(entry.getKey()));
            write(codec.encodeValue(entry.getValue()));
        }

        return this;
    }

    public CommandArgs<K, V> add(String s) {
        return write(s);
    }

    public CommandArgs<K, V> add(long n) {
        return write(Long.toString(n));
    }

    public CommandArgs<K, V> add(double n) {
        return write(Double.toString(n));
    }

    public CommandArgs<K, V> add(byte[] value) {
        return write(value);
    }

    public CommandArgs<K, V> add(CommandKeyword keyword) {
        return write(keyword.bytes);
    }

    public CommandArgs<K, V> add(CommandType type) {
        return write(type.bytes);
    }

    private CommandArgs<K, V> write(byte[] arg) {
        while (buffer.remaining() < (arg.length + 16)) {
            realloc(buffer.capacity() * 2);
        }

        buffer.put((byte) '$');
        writeLong(arg.length);
        buffer.put(CRLF);
        buffer.put(arg);
        buffer.put(CRLF);

        count++;
        return this;
    }

    private CommandArgs<K, V> write(String arg) {
        int length = arg.length();

        while (buffer.remaining() < (length + 16)) {
            realloc(buffer.capacity() * 2);
        }

        buffer.put((byte) '$');
        writeLong(length);
        buffer.put(CRLF);
        for (int i = 0; i < length; i++) {
            buffer.put((byte) arg.charAt(i));
        }
        buffer.put(CRLF);

        count++;
        return this;
    }

    //write a long in decimal form, 10 bytes or less
    private void writeLong(long value) {
        if (value < 10) {
            buffer.put((byte) ('0' + value));
            return;
        }

        StringBuilder sb = new StringBuilder(10);
        while (value > 0) {
            long digit = value % 10;
            sb.append((char) ('0' + digit));
            value /= 10;
        }

        for (int i = sb.length() - 1; i >= 0; i--) {
            buffer.put((byte) sb.charAt(i));
        }
    }

    private void realloc(int size) {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        this.buffer.flip();
        buffer.put(this.buffer);
        this.buffer = buffer;
    }
}
