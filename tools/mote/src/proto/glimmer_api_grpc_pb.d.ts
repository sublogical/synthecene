// package: glimmer
// file: glimmer_api.proto

/* tslint:disable */
/* eslint-disable */

import * as grpc from "@grpc/grpc-js";
import * as glimmer_api_pb from "./glimmer_api_pb";

interface IGlimmerService extends grpc.ServiceDefinition<grpc.UntypedServiceImplementation> {
    getNotifications: IGlimmerService_IGetNotifications;
    reportContext: IGlimmerService_IReportContext;
    syncChannel: IGlimmerService_ISyncChannel;
}

interface IGlimmerService_IGetNotifications extends grpc.MethodDefinition<glimmer_api_pb.NotificationRequest, glimmer_api_pb.NotificationResponse> {
    path: "/glimmer.Glimmer/GetNotifications";
    requestStream: false;
    responseStream: true;
    requestSerialize: grpc.serialize<glimmer_api_pb.NotificationRequest>;
    requestDeserialize: grpc.deserialize<glimmer_api_pb.NotificationRequest>;
    responseSerialize: grpc.serialize<glimmer_api_pb.NotificationResponse>;
    responseDeserialize: grpc.deserialize<glimmer_api_pb.NotificationResponse>;
}
interface IGlimmerService_IReportContext extends grpc.MethodDefinition<glimmer_api_pb.ContextRequest, glimmer_api_pb.ContextResponse> {
    path: "/glimmer.Glimmer/ReportContext";
    requestStream: false;
    responseStream: false;
    requestSerialize: grpc.serialize<glimmer_api_pb.ContextRequest>;
    requestDeserialize: grpc.deserialize<glimmer_api_pb.ContextRequest>;
    responseSerialize: grpc.serialize<glimmer_api_pb.ContextResponse>;
    responseDeserialize: grpc.deserialize<glimmer_api_pb.ContextResponse>;
}
interface IGlimmerService_ISyncChannel extends grpc.MethodDefinition<glimmer_api_pb.ChannelMessage, glimmer_api_pb.ChannelResponse> {
    path: "/glimmer.Glimmer/SyncChannel";
    requestStream: true;
    responseStream: true;
    requestSerialize: grpc.serialize<glimmer_api_pb.ChannelMessage>;
    requestDeserialize: grpc.deserialize<glimmer_api_pb.ChannelMessage>;
    responseSerialize: grpc.serialize<glimmer_api_pb.ChannelResponse>;
    responseDeserialize: grpc.deserialize<glimmer_api_pb.ChannelResponse>;
}

export const GlimmerService: IGlimmerService;

export interface IGlimmerServer extends grpc.UntypedServiceImplementation {
    getNotifications: grpc.handleServerStreamingCall<glimmer_api_pb.NotificationRequest, glimmer_api_pb.NotificationResponse>;
    reportContext: grpc.handleUnaryCall<glimmer_api_pb.ContextRequest, glimmer_api_pb.ContextResponse>;
    syncChannel: grpc.handleBidiStreamingCall<glimmer_api_pb.ChannelMessage, glimmer_api_pb.ChannelResponse>;
}

export interface IGlimmerClient {
    getNotifications(request: glimmer_api_pb.NotificationRequest, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<glimmer_api_pb.NotificationResponse>;
    getNotifications(request: glimmer_api_pb.NotificationRequest, metadata?: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<glimmer_api_pb.NotificationResponse>;
    reportContext(request: glimmer_api_pb.ContextRequest, callback: (error: grpc.ServiceError | null, response: glimmer_api_pb.ContextResponse) => void): grpc.ClientUnaryCall;
    reportContext(request: glimmer_api_pb.ContextRequest, metadata: grpc.Metadata, callback: (error: grpc.ServiceError | null, response: glimmer_api_pb.ContextResponse) => void): grpc.ClientUnaryCall;
    reportContext(request: glimmer_api_pb.ContextRequest, metadata: grpc.Metadata, options: Partial<grpc.CallOptions>, callback: (error: grpc.ServiceError | null, response: glimmer_api_pb.ContextResponse) => void): grpc.ClientUnaryCall;
    syncChannel(): grpc.ClientDuplexStream<glimmer_api_pb.ChannelMessage, glimmer_api_pb.ChannelResponse>;
    syncChannel(options: Partial<grpc.CallOptions>): grpc.ClientDuplexStream<glimmer_api_pb.ChannelMessage, glimmer_api_pb.ChannelResponse>;
    syncChannel(metadata: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientDuplexStream<glimmer_api_pb.ChannelMessage, glimmer_api_pb.ChannelResponse>;
}

export class GlimmerClient extends grpc.Client implements IGlimmerClient {
    constructor(address: string, credentials: grpc.ChannelCredentials, options?: Partial<grpc.ClientOptions>);
    public getNotifications(request: glimmer_api_pb.NotificationRequest, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<glimmer_api_pb.NotificationResponse>;
    public getNotifications(request: glimmer_api_pb.NotificationRequest, metadata?: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<glimmer_api_pb.NotificationResponse>;
    public reportContext(request: glimmer_api_pb.ContextRequest, callback: (error: grpc.ServiceError | null, response: glimmer_api_pb.ContextResponse) => void): grpc.ClientUnaryCall;
    public reportContext(request: glimmer_api_pb.ContextRequest, metadata: grpc.Metadata, callback: (error: grpc.ServiceError | null, response: glimmer_api_pb.ContextResponse) => void): grpc.ClientUnaryCall;
    public reportContext(request: glimmer_api_pb.ContextRequest, metadata: grpc.Metadata, options: Partial<grpc.CallOptions>, callback: (error: grpc.ServiceError | null, response: glimmer_api_pb.ContextResponse) => void): grpc.ClientUnaryCall;
    public syncChannel(options?: Partial<grpc.CallOptions>): grpc.ClientDuplexStream<glimmer_api_pb.ChannelMessage, glimmer_api_pb.ChannelResponse>;
    public syncChannel(metadata?: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientDuplexStream<glimmer_api_pb.ChannelMessage, glimmer_api_pb.ChannelResponse>;
}
