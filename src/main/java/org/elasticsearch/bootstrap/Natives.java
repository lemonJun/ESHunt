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

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

/**
 * The Natives class is a wrapper class that checks if the classes necessary for calling native methods are available on
 * startup. If they are not available, this class will avoid calling code that loads these classes.
 */
class Natives {
    private static final ESLogger logger = Loggers.getLogger(Natives.class);

    // marker to determine if the JNA class files are available to the JVM
    private static boolean jnaAvailable = false;

    static {
        try {
            // load the JNA version class to see if the JNA is available. this does not ensure that native libraries are
            // available
            Class.forName("com.sun.jna.Version");
            jnaAvailable = true;
        } catch (ClassNotFoundException e) {
            logger.warn("JNA not found. native methods will be disabled.");
        }
    }

    static void tryMlockall() {
        if (!jnaAvailable) {
            logger.warn("cannot mlockall because JNA is not available");
            return;
        }
        JNANatives.tryMlockall();
    }

    static void tryVirtualLock() {
        if (!jnaAvailable) {
            logger.warn("cannot mlockall because JNA is not available");
            return;
        }
        JNANatives.tryVirtualLock();
    }

    static void addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
        if (!jnaAvailable) {
            logger.warn("cannot register console handler because JNA is not available");
            return;
        }
        JNANatives.addConsoleCtrlHandler(handler);
    }

    static boolean isMemoryLocked() {
        if (!jnaAvailable) {
            return false;
        }
        return JNANatives.LOCAL_MLOCKALL;
    }
}
