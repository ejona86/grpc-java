/*
 * Copyright 2015, Google Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *
 *    * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.grpc.netty;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import io.grpc.Status;
import io.netty.handler.codec.http2.Http2FrameStream;

/**
 * Command sent from a Netty server stream to the handler to cancel the stream.
 */
class CancelServerStreamCommand extends WriteQueue.AbstractQueuedCommand {
  private final Http2FrameStream http2Stream;
  private final Status reason;

  CancelServerStreamCommand(Http2FrameStream http2Stream, Status reason) {
    this.http2Stream = Preconditions.checkNotNull(http2Stream, "http2Stream");
    this.reason = Preconditions.checkNotNull(reason, "reason");
  }

  @SuppressWarnings("unchecked")
  Http2FrameStream http2Stream() {
    return http2Stream;
  }

  Status reason() {
    return reason;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CancelServerStreamCommand that = (CancelServerStreamCommand) o;

    return Objects.equal(this.http2Stream, that.http2Stream)
        && Objects.equal(this.reason, that.reason);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(http2Stream, reason);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("stream", http2Stream)
        .add("reason", reason)
        .toString();
  }
}
