/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.flinktools.util;

import org.apache.flink.types.Row;

/**
 * A Flink Row that can be used with keyBy.
 * Elements of the Row can be (almost) any data type including JsonNode of string, long, and object.
 */
public class ComparableRow implements Comparable<ComparableRow> {
    public ComparableRow(int arity) {
        super();
    }

    @Override
    public int compareTo(ComparableRow t) {
        throw new UnsupportedOperationException();
    }
}
