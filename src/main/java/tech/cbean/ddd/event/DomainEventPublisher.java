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

import java.util.*;
import java.util.function.Consumer;

public class DomainEventPublisher {
    private static final DomainEventPublisher SELF = new DomainEventPublisher();
    private Map<Class<?>, List<Consumer<?>>> subscribes = new HashMap<>();

    public static DomainEventPublisher instance() {
        return SELF;
    }

    public <T> void publish(T event) {
        if (subscribes.isEmpty() || !subscribes.containsKey(event.getClass())) {
            return;
        }

        for (Consumer<?> consumer : subscribes.get(event.getClass())) {
            ((Consumer<T>) consumer).accept(event);
        }
    }

    public <T> void subscribe(Class<T> t, Consumer<T> consumer) {
        Optional.ofNullable(subscribes.get(t)).orElseGet(() -> {
            subscribes.put(t, new ArrayList<>());
            return subscribes.get(t);
        }).add(consumer);
    }

    public <T> void subscribe(String clazz, Consumer<T> consumer) {
        try {
            Class<?> t = Class.forName(clazz);
            Optional.ofNullable(subscribes.get(t)).orElseGet(() -> {
                subscribes.put(t, new ArrayList<>());
                return subscribes.get(t);
            }).add(consumer);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Do not found class " + clazz, e);
        }
    }
}
