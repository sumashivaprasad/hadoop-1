package si.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.18.0)",
    comments = "Source: si.proto")
public final class SchedulerGrpc {

  private SchedulerGrpc() {}

  public static final String SERVICE_NAME = "si.v1.Scheduler";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<si.v1.Si.RegisterResourceManagerRequest,
      si.v1.Si.RegisterResourceManagerResponse> getRegisterResourceManagerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RegisterResourceManager",
      requestType = si.v1.Si.RegisterResourceManagerRequest.class,
      responseType = si.v1.Si.RegisterResourceManagerResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<si.v1.Si.RegisterResourceManagerRequest,
      si.v1.Si.RegisterResourceManagerResponse> getRegisterResourceManagerMethod() {
    io.grpc.MethodDescriptor<si.v1.Si.RegisterResourceManagerRequest, si.v1.Si.RegisterResourceManagerResponse> getRegisterResourceManagerMethod;
    if ((getRegisterResourceManagerMethod = SchedulerGrpc.getRegisterResourceManagerMethod) == null) {
      synchronized (SchedulerGrpc.class) {
        if ((getRegisterResourceManagerMethod = SchedulerGrpc.getRegisterResourceManagerMethod) == null) {
          SchedulerGrpc.getRegisterResourceManagerMethod = getRegisterResourceManagerMethod = 
              io.grpc.MethodDescriptor.<si.v1.Si.RegisterResourceManagerRequest, si.v1.Si.RegisterResourceManagerResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "si.v1.Scheduler", "RegisterResourceManager"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  si.v1.Si.RegisterResourceManagerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  si.v1.Si.RegisterResourceManagerResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new SchedulerMethodDescriptorSupplier("RegisterResourceManager"))
                  .build();
          }
        }
     }
     return getRegisterResourceManagerMethod;
  }

  private static volatile io.grpc.MethodDescriptor<si.v1.Si.UpdateRequest,
      si.v1.Si.UpdateResponse> getUpdateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Update",
      requestType = si.v1.Si.UpdateRequest.class,
      responseType = si.v1.Si.UpdateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<si.v1.Si.UpdateRequest,
      si.v1.Si.UpdateResponse> getUpdateMethod() {
    io.grpc.MethodDescriptor<si.v1.Si.UpdateRequest, si.v1.Si.UpdateResponse> getUpdateMethod;
    if ((getUpdateMethod = SchedulerGrpc.getUpdateMethod) == null) {
      synchronized (SchedulerGrpc.class) {
        if ((getUpdateMethod = SchedulerGrpc.getUpdateMethod) == null) {
          SchedulerGrpc.getUpdateMethod = getUpdateMethod = 
              io.grpc.MethodDescriptor.<si.v1.Si.UpdateRequest, si.v1.Si.UpdateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(
                  "si.v1.Scheduler", "Update"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  si.v1.Si.UpdateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  si.v1.Si.UpdateResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new SchedulerMethodDescriptorSupplier("Update"))
                  .build();
          }
        }
     }
     return getUpdateMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static SchedulerStub newStub(io.grpc.Channel channel) {
    return new SchedulerStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static SchedulerBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new SchedulerBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static SchedulerFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new SchedulerFutureStub(channel);
  }

  /**
   */
  public static abstract class SchedulerImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Register a new RM, if it is a reconnect from previous RM, cleanup
     * all in-memory data and resync with RM.
     * </pre>
     */
    public void registerResourceManager(si.v1.Si.RegisterResourceManagerRequest request,
        io.grpc.stub.StreamObserver<si.v1.Si.RegisterResourceManagerResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRegisterResourceManagerMethod(), responseObserver);
    }

    /**
     * <pre>
     * Update Scheduler status (including node status update, allocation request
     * updates, etc. And receive updates from scheduler for allocation changes,
     * any required status changes, etc.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<si.v1.Si.UpdateRequest> update(
        io.grpc.stub.StreamObserver<si.v1.Si.UpdateResponse> responseObserver) {
      return asyncUnimplementedStreamingCall(getUpdateMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getRegisterResourceManagerMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                si.v1.Si.RegisterResourceManagerRequest,
                si.v1.Si.RegisterResourceManagerResponse>(
                  this, METHODID_REGISTER_RESOURCE_MANAGER)))
          .addMethod(
            getUpdateMethod(),
            asyncBidiStreamingCall(
              new MethodHandlers<
                si.v1.Si.UpdateRequest,
                si.v1.Si.UpdateResponse>(
                  this, METHODID_UPDATE)))
          .build();
    }
  }

  /**
   */
  public static final class SchedulerStub extends io.grpc.stub.AbstractStub<SchedulerStub> {
    private SchedulerStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SchedulerStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SchedulerStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SchedulerStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a new RM, if it is a reconnect from previous RM, cleanup
     * all in-memory data and resync with RM.
     * </pre>
     */
    public void registerResourceManager(si.v1.Si.RegisterResourceManagerRequest request,
        io.grpc.stub.StreamObserver<si.v1.Si.RegisterResourceManagerResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRegisterResourceManagerMethod(), responseObserver);
    }

    /**
     * <pre>
     * Update Scheduler status (including node status update, allocation request
     * updates, etc. And receive updates from scheduler for allocation changes,
     * any required status changes, etc.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<si.v1.Si.UpdateRequest> update(
        io.grpc.stub.StreamObserver<si.v1.Si.UpdateResponse> responseObserver) {
      return asyncUnimplementedStreamingCall(getUpdateMethod(), responseObserver);
    }
  }

  /**
   */
  public static final class SchedulerBlockingStub extends io.grpc.stub.AbstractStub<SchedulerBlockingStub> {
    private SchedulerBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SchedulerBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SchedulerBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SchedulerBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a new RM, if it is a reconnect from previous RM, cleanup
     * all in-memory data and resync with RM.
     * </pre>
     */
    public si.v1.Si.RegisterResourceManagerResponse registerResourceManager(si.v1.Si.RegisterResourceManagerRequest request) {
      return blockingUnaryCall(
          getChannel(), getRegisterResourceManagerMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class SchedulerFutureStub extends io.grpc.stub.AbstractStub<SchedulerFutureStub> {
    private SchedulerFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SchedulerFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SchedulerFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SchedulerFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a new RM, if it is a reconnect from previous RM, cleanup
     * all in-memory data and resync with RM.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<si.v1.Si.RegisterResourceManagerResponse> registerResourceManager(
        si.v1.Si.RegisterResourceManagerRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRegisterResourceManagerMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_REGISTER_RESOURCE_MANAGER = 0;
  private static final int METHODID_UPDATE = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final SchedulerImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(SchedulerImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_REGISTER_RESOURCE_MANAGER:
          serviceImpl.registerResourceManager((si.v1.Si.RegisterResourceManagerRequest) request,
              (io.grpc.stub.StreamObserver<si.v1.Si.RegisterResourceManagerResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_UPDATE:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.update(
              (io.grpc.stub.StreamObserver<si.v1.Si.UpdateResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class SchedulerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    SchedulerBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return si.v1.Si.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Scheduler");
    }
  }

  private static final class SchedulerFileDescriptorSupplier
      extends SchedulerBaseDescriptorSupplier {
    SchedulerFileDescriptorSupplier() {}
  }

  private static final class SchedulerMethodDescriptorSupplier
      extends SchedulerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    SchedulerMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (SchedulerGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new SchedulerFileDescriptorSupplier())
              .addMethod(getRegisterResourceManagerMethod())
              .addMethod(getUpdateMethod())
              .build();
        }
      }
    }
    return result;
  }
}
