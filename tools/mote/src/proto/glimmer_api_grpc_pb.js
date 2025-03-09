// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('@grpc/grpc-js');
var glimmer_api_pb = require('./glimmer_api_pb.js');

function serialize_glimmer_ChannelMessage(arg) {
  if (!(arg instanceof glimmer_api_pb.ChannelMessage)) {
    throw new Error('Expected argument of type glimmer.ChannelMessage');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_glimmer_ChannelMessage(buffer_arg) {
  return glimmer_api_pb.ChannelMessage.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_glimmer_ChannelResponse(arg) {
  if (!(arg instanceof glimmer_api_pb.ChannelResponse)) {
    throw new Error('Expected argument of type glimmer.ChannelResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_glimmer_ChannelResponse(buffer_arg) {
  return glimmer_api_pb.ChannelResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_glimmer_ContextRequest(arg) {
  if (!(arg instanceof glimmer_api_pb.ContextRequest)) {
    throw new Error('Expected argument of type glimmer.ContextRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_glimmer_ContextRequest(buffer_arg) {
  return glimmer_api_pb.ContextRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_glimmer_ContextResponse(arg) {
  if (!(arg instanceof glimmer_api_pb.ContextResponse)) {
    throw new Error('Expected argument of type glimmer.ContextResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_glimmer_ContextResponse(buffer_arg) {
  return glimmer_api_pb.ContextResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_glimmer_NotificationRequest(arg) {
  if (!(arg instanceof glimmer_api_pb.NotificationRequest)) {
    throw new Error('Expected argument of type glimmer.NotificationRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_glimmer_NotificationRequest(buffer_arg) {
  return glimmer_api_pb.NotificationRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_glimmer_NotificationResponse(arg) {
  if (!(arg instanceof glimmer_api_pb.NotificationResponse)) {
    throw new Error('Expected argument of type glimmer.NotificationResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_glimmer_NotificationResponse(buffer_arg) {
  return glimmer_api_pb.NotificationResponse.deserializeBinary(new Uint8Array(buffer_arg));
}


var GlimmerService = exports.GlimmerService = {
  // Stream of notifications from the server to the client
getNotifications: {
    path: '/glimmer.Glimmer/GetNotifications',
    requestStream: false,
    responseStream: true,
    requestType: glimmer_api_pb.NotificationRequest,
    responseType: glimmer_api_pb.NotificationResponse,
    requestSerialize: serialize_glimmer_NotificationRequest,
    requestDeserialize: deserialize_glimmer_NotificationRequest,
    responseSerialize: serialize_glimmer_NotificationResponse,
    responseDeserialize: deserialize_glimmer_NotificationResponse,
  },
  // Report the current context to the server
reportContext: {
    path: '/glimmer.Glimmer/ReportContext',
    requestStream: false,
    responseStream: false,
    requestType: glimmer_api_pb.ContextRequest,
    responseType: glimmer_api_pb.ContextResponse,
    requestSerialize: serialize_glimmer_ContextRequest,
    requestDeserialize: deserialize_glimmer_ContextRequest,
    responseSerialize: serialize_glimmer_ContextResponse,
    responseDeserialize: deserialize_glimmer_ContextResponse,
  },
  // Bidirectional streaming for channel synchronization
syncChannel: {
    path: '/glimmer.Glimmer/SyncChannel',
    requestStream: true,
    responseStream: true,
    requestType: glimmer_api_pb.ChannelMessage,
    responseType: glimmer_api_pb.ChannelResponse,
    requestSerialize: serialize_glimmer_ChannelMessage,
    requestDeserialize: deserialize_glimmer_ChannelMessage,
    responseSerialize: serialize_glimmer_ChannelResponse,
    responseDeserialize: deserialize_glimmer_ChannelResponse,
  },
};

exports.GlimmerClient = grpc.makeGenericClientConstructor(GlimmerService, 'Glimmer');
