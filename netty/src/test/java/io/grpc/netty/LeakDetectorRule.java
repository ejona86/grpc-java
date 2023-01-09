/*
 * Copyright 2023 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.netty;

import io.netty.util.ResourceLeakDetector;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.rules.ExternalResource;

/**
 * Enables paranoid leak detection and checks whether ResourceLeakDetector logged a failure.
 */
final class LeakDetectorRule extends ExternalResource {
  private Handler handler = new MarkFailedHandler();
  private ResourceLeakDetector.Level originalLevel;
  private boolean failed;

  @Override
  protected void before() {
    originalLevel = ResourceLeakDetector.getLevel();
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    Logger.getLogger(ResourceLeakDetector.class.getName()).addHandler(handler);
  }

  @Override
  protected void after() {
    System.gc();
    /*try {
      Thread.sleep(100);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new AssertionError(ex);
    }*/
    Logger.getLogger(ResourceLeakDetector.class.getName()).removeHandler(handler);
    ResourceLeakDetector.setLevel(originalLevel);
    if (failed) {
      throw new AssertionError("Leak detected");
    }
  }

  final class MarkFailedHandler extends Handler {
    @Override public void publish(java.util.logging.LogRecord record) {
      failed = true;
    }

    @Override public void flush() {}

    @Override public void close() {}
  }
}
