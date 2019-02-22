package org.apache.hadoop.yarn.server.externalscheduler;

import io.grpc.ManagedChannel;

import io.grpc.netty.NettyChannelBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import si.v1.SchedulerGrpc;

import java.util.concurrent.TimeUnit;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public final class ExternalSchedulerGrpcClient implements AutoCloseable {

  private static final Log LOG = LogFactory.getLog(ExternalSchedulerGrpcClient.class);

  private final ManagedChannel channel;


  private ExternalSchedulerGrpcClient(ManagedChannel channel) {
    this.channel = channel;
  }

  public ExternalSchedulerGrpcClient(String host, int port) {
    this(NettyChannelBuilder.forAddress(host, port)
        // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
        // needing certificates.
        //TODO - Enable SSL
        //TODO - Authenticate to Unity scheduler using JWT/KRB
        .usePlaintext()
        .build());
  }

  /**
   * Shutdown the communication channel gracefully,
   * wait for 5 seconds before it is enforced.
   */
  @Override
  public void close() {
    try {
      this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Failed to gracefully shutdown"
          + " gRPC communication channel in 5 seconds", e);
    }
  }

  /**
   * Creates a blocking stub for Unity Scheduler on the given channel.
   * @return the blocking stub
   */
  public SchedulerGrpc.SchedulerBlockingStub createSchedulerBlockingStub() {
    return SchedulerGrpc.newBlockingStub(channel);
  }

  /**
   * Creates a blocking stub for Unity Scheduler on the given channel.
   * @return the blocking stub
   */
  public SchedulerGrpc.SchedulerStub createSchedulerStub() {
    return SchedulerGrpc.newStub(channel);
  }


}

