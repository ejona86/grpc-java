/*
 * Copyright 2014, Google Inc. All rights reserved.
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.netty.Utils.CONTENT_TYPE_HEADER;
import static io.grpc.netty.Utils.HTTP_METHOD;
import static io.grpc.netty.Utils.TE_HEADER;
import static io.grpc.netty.Utils.TE_TRAILERS;
import static io.netty.handler.codec.http2.Http2CodecUtil.toByteBuf;
import static io.netty.handler.codec.http2.Http2Error.INTERNAL_ERROR;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.ServerStreamListener;
import io.grpc.internal.ServerTransportListener;
import io.grpc.internal.StatsTraceContext;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2GoAwayFrame;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.DefaultHttp2ResetFrame;
import io.netty.handler.codec.http2.DefaultHttp2WindowUpdateFrame;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception.StreamException;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameStream;
import io.netty.handler.codec.http2.Http2FrameStreamException;
import io.netty.handler.codec.http2.Http2FrameStreamVisitor;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2PingFrame;
import io.netty.handler.codec.http2.Http2ResetFrame;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.logging.LogLevel;
import io.netty.util.ReferenceCountUtil;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * Server-side Netty handler for GRPC processing. All event handlers are executed entirely within
 * the context of the Netty Channel thread.
 */
class NettyServerHandler extends AbstractNettyHandler {
  private static Logger logger = Logger.getLogger(NettyServerHandler.class.getName());

  private final ServerTransportListener transportListener;
  private final int maxMessageSize;
  private Attributes attributes;
  private Throwable connectionError;
  private boolean teWarningLogged;
  private WriteQueue serverWriteQueue;
  private Attributes protocolNegotationAttrs = Attributes.EMPTY;

  static NettyServerHandler newHandler(ServerTransportListener transportListener,
                                       int maxStreams,
                                       int flowControlWindow,
                                       int maxHeaderListSize,
                                       int maxMessageSize) {
    Http2FrameCodecBuilder builder = Http2FrameCodecBuilder
        .forServer()
        .frameLogger(new Http2FrameLogger(LogLevel.DEBUG, NettyServerHandler.class));

    return newHandler(builder, transportListener, maxStreams, flowControlWindow, maxHeaderListSize,
        maxMessageSize);
  }

  @VisibleForTesting
  static NettyServerHandler newHandler(Http2FrameCodecBuilder frameCodecBuilder,
                                       ServerTransportListener transportListener,
                                       int maxStreams,
                                       int flowControlWindow,
                                       int maxHeaderListSize,
                                       int maxMessageSize) {
    Preconditions.checkArgument(maxStreams > 0, "maxStreams must be positive");
    Preconditions.checkArgument(flowControlWindow > 0, "flowControlWindow must be positive");
    Preconditions.checkArgument(maxHeaderListSize > 0, "maxHeaderListSize must be positive");
    Preconditions.checkArgument(maxMessageSize > 0, "maxMessageSize must be positive");

    frameCodecBuilder.initialSettings(
        new Http2Settings().initialWindowSize(flowControlWindow).maxConcurrentStreams(maxStreams)
            .maxHeaderListSize(maxHeaderListSize));

    return new NettyServerHandler(frameCodecBuilder, flowControlWindow,
        transportListener, maxMessageSize);
  }

  private NettyServerHandler(Http2FrameCodecBuilder frameCodecBuilder, int flowControlWindow,
      ServerTransportListener transportListener, int maxMessageSize) {
    super(frameCodecBuilder, flowControlWindow);
    checkArgument(maxMessageSize >= 0, "maxMessageSize must be >= 0");
    this.maxMessageSize = maxMessageSize;
    this.transportListener = checkNotNull(transportListener, "transportListener");
  }

  @Nullable
  Throwable connectionError() {
    return connectionError;
  }

  @Override
  public void handlerAdded0(ChannelHandlerContext ctx) throws Exception {
    serverWriteQueue = new WriteQueue(ctx.channel());
    attributes = transportListener.transportReady(Attributes.newBuilder(protocolNegotationAttrs)
        .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, ctx.channel().remoteAddress())
        .build());
    super.handlerAdded0(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof Http2HeadersFrame) {
      onHeadersRead(ctx, (Http2HeadersFrame) msg);
    } else if (msg instanceof Http2DataFrame) {
      onDataRead((Http2DataFrame) msg);
    } else if (msg instanceof Http2ResetFrame) {
      onRstStreamRead((Http2ResetFrame) msg);
    } else if (msg instanceof Http2PingFrame) {
      Http2PingFrame pingFrame = (Http2PingFrame) msg;
      if (pingFrame.ack()) {
        onPingAckRead(pingFrame.content());
      }
    }
  }

  private void onHeadersRead(ChannelHandlerContext ctx, Http2HeadersFrame frame) throws Exception {
    final Http2Headers headers = frame.headers();

    if (!teWarningLogged && !TE_TRAILERS.equals(headers.get(TE_HEADER))) {
      logger.warning(String.format("Expected header TE: %s, but %s is received. This means "
            + "some intermediate proxy may not support trailers",
            TE_TRAILERS, headers.get(TE_HEADER)));
      teWarningLogged = true;
    }

    Http2FrameStream http2Stream = frame.stream();

    try {
      // Verify that the Content-Type is correct in the request.
      verifyContentType(http2Stream, headers);
      String method = determineMethod(http2Stream.id(), headers);
      Metadata metadata = Utils.convertHeaders(headers);
      StatsTraceContext statsTraceCtx =
          checkNotNull(transportListener.methodDetermined(method, metadata), "statsTraceCtx");
      NettyServerStream.TransportState state = new NettyServerStream.TransportState(
          this, http2Stream, maxMessageSize, statsTraceCtx);
      NettyServerStream stream = new NettyServerStream(ctx.channel(), state, attributes,
          statsTraceCtx);
      ServerStreamListener listener = transportListener.streamCreated(stream, method, metadata);
      // TODO(ejona): this could be racy since stream could have been used before getting here. All
      // cases appear to be fine, but some are almost only by happenstance and it is difficult to
      // audit. It would be good to improve the API to be less prone to races.
      state.setListener(listener);
      registerTransportState(http2Stream, state);
      //http2Stream.closeFuture().deregisterWhenDestroyed(); // FIXME
    } catch (Http2Exception e) {
      throw e;
    } catch (Throwable e) {
      logger.log(Level.WARNING, "Exception in onHeadersRead()", e);
      // Throw an exception that will get handled by onStreamError.
      throw newStreamException(http2Stream, e);
    }
  }

  private void onDataRead(Http2DataFrame data) throws Http2FrameStreamException {
    flowControlPing().onDataRead(data.content().readableBytes(), data.padding());
    try {
      NettyServerStream.TransportState stream = requireServerStream(data.stream());
      stream.inboundDataReceived(data.content(), data.isEndStream());
    } catch (Throwable e) {
      logger.log(Level.WARNING, "Exception in onDataRead()", e);
      // Throw an exception that will get handled by onStreamError.
      throw newStreamException(data.stream(), e);
    }
  }

  private void onRstStreamRead(Http2ResetFrame frame) throws Http2FrameStreamException {
    Http2FrameStream http2Stream = frame.stream();
    try {
      NettyServerStream.TransportState stream = serverStream(http2Stream);
      if (stream != null) {
        stream.transportReportStatus(Status.CANCELLED);
      }
    } catch (Throwable e) {
      logger.log(Level.WARNING, "Exception in onRstStreamRead()", e);
      // Throw an exception that will get handled by onStreamError.
      throw newStreamException(http2Stream, e);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause instanceof Http2FrameStreamException) {
      logger.log(Level.WARNING, "Stream Error", cause);
      Http2FrameStreamException http2Exception = (Http2FrameStreamException) cause;
      NettyServerStream.TransportState serverStream = serverStream(http2Exception.stream());

      // No managed state might be attached, when an exception is thrown before the server stream
      // has been properly initialized. Most likely if header validation failed.
      if (serverStream != null) {
        serverStream.transportReportStatus(Utils.statusFromThrowable(cause));
      }

      ctx.write(new DefaultHttp2ResetFrame(http2Exception.error()).stream(http2Exception.stream()));
    } else {
      logger.log(Level.WARNING, "Connection Error", cause);
      connectionError = cause;
      Http2Exception http2Exception = Http2CodecUtil.getEmbeddedHttp2Exception(cause);
      if (http2Exception == null) {
        http2Exception = new Http2Exception(INTERNAL_ERROR, cause.getMessage(), cause);
      }
      ctx.write(
          new DefaultHttp2GoAwayFrame(http2Exception.error(), toByteBuf(ctx, http2Exception)));
      ctx.close();
    }
  }

  public void onPingAckRead(long payload) throws Http2Exception {
    if (payload == flowControlPing().payload()) {
      flowControlPing().updateWindow();
      if (logger.isLoggable(Level.FINE)) {
        logger.log(Level.FINE, String.format("Window: %d",
            flowControlPing().initialConnectionWindow()));
      }
    } else {
      logger.warning("Received unexpected ping ack. No ping outstanding");
    }
  }

  @Override
  public void handleProtocolNegotiationCompleted(Attributes attrs) {
    this.protocolNegotationAttrs = attrs;
  }

  /**
   * Handler for the Channel shutting down.
   */
  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    try {
      final Status status =
          Status.UNAVAILABLE.withDescription("connection terminated for unknown reason");
      forEachActiveStream(new Http2FrameStreamVisitor() {
        @Override
        public boolean visit(Http2FrameStream http2Stream) {
          NettyServerStream.TransportState serverStream = serverStream(http2Stream);
          if (serverStream != null) {
            serverStream.transportReportStatus(status);
          }
          return true;
        }
      });
    } finally {
      super.channelInactive(ctx);
    }
  }

  WriteQueue getWriteQueue() {
    return serverWriteQueue;
  }

  /**
   * Handler for commands sent from the stream.
   */
  // TODO(buchgr): In the write queue can merge some command and frame objects.
  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (msg instanceof SendGrpcFrameCommand) {
      sendGrpcFrame(ctx, (SendGrpcFrameCommand) msg, promise);
    } else if (msg instanceof SendResponseHeadersCommand) {
      sendResponseHeaders(ctx, (SendResponseHeadersCommand) msg, promise);
    } else if (msg instanceof RequestMessagesCommand) {
      ((RequestMessagesCommand) msg).requestMessages();
      promise.setSuccess();
    } else if (msg instanceof CancelServerStreamCommand) {
      cancelStream(ctx, (CancelServerStreamCommand) msg, promise);
    } else if (msg instanceof ForcefulCloseCommand) {
      forcefulClose(ctx, (ForcefulCloseCommand) msg, promise);
    } else {
      AssertionError e =
          new AssertionError("Write called for unexpected type: " + msg.getClass().getName());
      ReferenceCountUtil.release(msg);
      promise.setFailure(e);
      throw e;
    }
  }

  /**
   * Returns the given processed bytes back to inbound flow control.
   */
  void returnProcessedBytes(Http2FrameStream stream, int bytes) {
    ctx().write(new DefaultHttp2WindowUpdateFrame(bytes).stream(stream));
  }

  private void closeStreamWhenDone(ChannelPromise promise, Http2FrameStream http2Stream) {
    // TODO(ejona): This conversion from http2Stream is unnecessary. It's a hack to re-use
    // SendGrpcFrameCommand on client and server.
    final NettyServerStream.TransportState stream = requireServerStream(http2Stream);
    promise.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        stream.complete();
      }
    });
  }

  /**
   * Sends the given gRPC frame to the client.
   */
  private void sendGrpcFrame(ChannelHandlerContext ctx, SendGrpcFrameCommand cmd,
      ChannelPromise promise) {
    if (cmd.endStream()) {
      closeStreamWhenDone(promise, cmd.http2Stream());
    }
    Http2DataFrame dataFrame = new DefaultHttp2DataFrame(cmd.content(), cmd.endStream(), 0)
        .stream(cmd.http2Stream());
    ctx.write(dataFrame, promise);
  }

  /**
   * Sends the response headers to the client.
   */
  private void sendResponseHeaders(ChannelHandlerContext ctx, SendResponseHeadersCommand cmd,
      ChannelPromise promise) {
    if (cmd.endOfStream()) {
      closeStreamWhenDone(promise, cmd.stream().http2Stream());
    }
    Http2HeadersFrame headersFrame =
        new DefaultHttp2HeadersFrame(cmd.headers(), cmd.endOfStream(), 0)
            .stream(cmd.stream().http2Stream());
    ctx.write(headersFrame, promise);
  }

  private void cancelStream(ChannelHandlerContext ctx, CancelServerStreamCommand cmd,
      ChannelPromise promise) {
    // Notify the listener if we haven't already.
    cmd.stream().transportReportStatus(cmd.reason());
    // Terminate the stream.
    ctx.write(
        new DefaultHttp2ResetFrame(Http2Error.CANCEL).stream(cmd.stream().http2Stream()),
        promise);
  }

  private void forcefulClose(final ChannelHandlerContext ctx, final ForcefulCloseCommand msg,
      ChannelPromise promise) throws Exception {
    try {
      forEachActiveStream(new Http2FrameStreamVisitor() {
        @Override
        public boolean visit(Http2FrameStream stream) {
          NettyServerStream.TransportState serverStream = serverStream(stream);
          if (serverStream != null) {
            serverStream.transportReportStatus(msg.getStatus());
          }
          ctx.write(new DefaultHttp2ResetFrame(Http2Error.CANCEL).stream(stream), ctx.newPromise());
          return true;
        }
      });
    } finally {
      ctx.close(promise);
    }
  }

  private void verifyContentType(Http2FrameStream http2Stream, Http2Headers headers)
      throws Http2FrameStreamException {
    CharSequence contentType = headers.get(CONTENT_TYPE_HEADER);
    if (contentType == null) {
      StreamException e = (StreamException) Http2Exception.streamError(http2Stream.id(),
          Http2Error.REFUSED_STREAM, "Content-Type is missing from the request");
      throw new Http2FrameStreamException(http2Stream, Http2Error.REFUSED_STREAM, e);
    }
    String contentTypeString = contentType.toString();
    if (!GrpcUtil.isGrpcContentType(contentTypeString)) {
      StreamException e = (StreamException) Http2Exception.streamError(http2Stream.id(),
          Http2Error.REFUSED_STREAM, "Content-Type '%s' is not supported", contentTypeString);
      throw new Http2FrameStreamException(http2Stream, Http2Error.REFUSED_STREAM, e);
    }
  }

  private String determineMethod(int streamId, Http2Headers headers) throws Http2Exception {
    if (!HTTP_METHOD.equals(headers.method())) {
      throw Http2Exception.streamError(streamId, Http2Error.REFUSED_STREAM,
          "Method '%s' is not supported", headers.method());
    }
    // Remove the leading slash of the path and get the fully qualified method name
    CharSequence path = headers.path();
    if (path.charAt(0) != '/') {
      throw Http2Exception.streamError(streamId, Http2Error.REFUSED_STREAM,
          "Malformatted path: %s", path);
    }
    return path.subSequence(1, path.length()).toString();
  }

  /**
   * Returns the server stream associated to the given HTTP/2 stream object.
   */
  private NettyServerStream.TransportState serverStream(Http2FrameStream stream) {
    return (NettyServerStream.TransportState) transportState(stream);
  }

  private NettyServerStream.TransportState requireServerStream(Http2FrameStream stream) {
    NettyServerStream.TransportState state = serverStream(stream);
    if (state == null) {
      throw new IllegalStateException("Could not find server stream state: " + stream.id());
    }
    return state;
  }

  private static Http2FrameStreamException newStreamException(Http2FrameStream http2Stream,
      Throwable cause) {
    if (cause instanceof Http2FrameStreamException) {
      return (Http2FrameStreamException) cause;
    }
    return new Http2FrameStreamException(http2Stream, INTERNAL_ERROR, cause);
  }
}
