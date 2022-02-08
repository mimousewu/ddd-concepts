/*
 * MIT License
 *
 * Copyright (c) 2022 Tao Wu. mimousewu@gmail.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package tech.cbean.ddd.event;

import tech.cbean.ddd.exception.DomainException;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class QueuedEventHolder<T> {
    public static boolean DEBUG = false;

    private int timeout;

    private BlockingQueue<T> queue;

    public QueuedEventHolder(int capacity, int timeout) {
        this.timeout = timeout;
        queue = new LinkedBlockingDeque<>(capacity);
    }

    public QueuedEventHolder(int capacity) {
        this(capacity, 2);
    }

    public void offer(T event) throws InterruptedException {
        queue.offer(event, timeout, TimeUnit.MINUTES);
    }

    public void takeStream(Consumer<Stream<T>> streamConsumer) {
        new Thread(() -> {
            while (true) {
                Stream<T> stream = IntStream.range(0, queue.size() - 1)
                        .mapToObj(i -> {
                            try {
                                return queue.take();
                            } catch (Throwable t) {
                                if (DEBUG) System.err.println(fulfillError("Handle send event failed", t));
                                return null;
                            }
                        })
                        .peek(e -> {
                            if (DEBUG) System.out.println("Sent out event: " + e);
                        })
                        .filter(Objects::nonNull);
                streamConsumer.accept(stream);
            }
        }).start();

        waitShutdownHook();
    }

    public void take(Consumer<T> consumer) {
        new Thread(() -> {
            while (true) {
                try {
                    T event = queue.take();
                    consumer.accept(event);
                    if (DEBUG) System.out.println("Sent out event: " + event);
                } catch (Throwable e) {
                    if (DEBUG) System.err.println(fulfillError("Handle send event failed", e));
                }
            }
        }).start();

        waitShutdownHook();
    }

    private String fulfillError(String message, Throwable cause) {
        return Optional.of(cause).filter(ex -> ex instanceof DomainException)
                .map(ex -> (DomainException) ex)
                .map(ex -> ex.getMessage() + " [" + ex.getDetails().entrySet().stream()
                        .map(en -> en.getKey() + ":" + en.getValue())
                        .collect(Collectors.joining(",")) + "]")
                .orElse(message);
    }

    private void waitShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            while (!queue.isEmpty()) {
                if (DEBUG) System.out.println("Some queued event waiting for resolve..." + queue.size());
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    if (DEBUG) System.err.println("Waiting for shutdown failed: " + e.getMessage());
                }
            }
            if (DEBUG) System.out.println("All queued event have been resolved!");
        }));
    }
}
