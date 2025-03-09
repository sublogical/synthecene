// package: glimmer
// file: glimmer_api.proto

/* tslint:disable */
/* eslint-disable */

import * as jspb from "google-protobuf";

export class NotificationRequest extends jspb.Message { 

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): NotificationRequest.AsObject;
    static toObject(includeInstance: boolean, msg: NotificationRequest): NotificationRequest.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: NotificationRequest, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): NotificationRequest;
    static deserializeBinaryFromReader(message: NotificationRequest, reader: jspb.BinaryReader): NotificationRequest;
}

export namespace NotificationRequest {
    export type AsObject = {
    }
}

export class NotificationResponse extends jspb.Message { 
    getMessage(): string;
    setMessage(value: string): NotificationResponse;
    getType(): string;
    setType(value: string): NotificationResponse;
    getTimestamp(): number;
    setTimestamp(value: number): NotificationResponse;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): NotificationResponse.AsObject;
    static toObject(includeInstance: boolean, msg: NotificationResponse): NotificationResponse.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: NotificationResponse, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): NotificationResponse;
    static deserializeBinaryFromReader(message: NotificationResponse, reader: jspb.BinaryReader): NotificationResponse;
}

export namespace NotificationResponse {
    export type AsObject = {
        message: string,
        type: string,
        timestamp: number,
    }
}

export class ContextRequest extends jspb.Message { 
    getUrl(): string;
    setUrl(value: string): ContextRequest;
    getTitle(): string;
    setTitle(value: string): ContextRequest;
    getContent(): string;
    setContent(value: string): ContextRequest;
    getTimestamp(): number;
    setTimestamp(value: number): ContextRequest;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): ContextRequest.AsObject;
    static toObject(includeInstance: boolean, msg: ContextRequest): ContextRequest.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: ContextRequest, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): ContextRequest;
    static deserializeBinaryFromReader(message: ContextRequest, reader: jspb.BinaryReader): ContextRequest;
}

export namespace ContextRequest {
    export type AsObject = {
        url: string,
        title: string,
        content: string,
        timestamp: number,
    }
}

export class ContextResponse extends jspb.Message { 
    getSuccess(): boolean;
    setSuccess(value: boolean): ContextResponse;
    getMessage(): string;
    setMessage(value: string): ContextResponse;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): ContextResponse.AsObject;
    static toObject(includeInstance: boolean, msg: ContextResponse): ContextResponse.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: ContextResponse, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): ContextResponse;
    static deserializeBinaryFromReader(message: ContextResponse, reader: jspb.BinaryReader): ContextResponse;
}

export namespace ContextResponse {
    export type AsObject = {
        success: boolean,
        message: string,
    }
}

export class ChannelRequest extends jspb.Message { 

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): ChannelRequest.AsObject;
    static toObject(includeInstance: boolean, msg: ChannelRequest): ChannelRequest.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: ChannelRequest, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): ChannelRequest;
    static deserializeBinaryFromReader(message: ChannelRequest, reader: jspb.BinaryReader): ChannelRequest;
}

export namespace ChannelRequest {
    export type AsObject = {
    }
}

export class ChannelMessage extends jspb.Message { 
    getChannelId(): string;
    setChannelId(value: string): ChannelMessage;
    getContent(): string;
    setContent(value: string): ChannelMessage;
    getTimestamp(): number;
    setTimestamp(value: number): ChannelMessage;
    getSenderId(): string;
    setSenderId(value: string): ChannelMessage;
    getType(): MessageType;
    setType(value: MessageType): ChannelMessage;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): ChannelMessage.AsObject;
    static toObject(includeInstance: boolean, msg: ChannelMessage): ChannelMessage.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: ChannelMessage, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): ChannelMessage;
    static deserializeBinaryFromReader(message: ChannelMessage, reader: jspb.BinaryReader): ChannelMessage;
}

export namespace ChannelMessage {
    export type AsObject = {
        channelId: string,
        content: string,
        timestamp: number,
        senderId: string,
        type: MessageType,
    }
}

export class ChannelResponse extends jspb.Message { 
    getChannelId(): string;
    setChannelId(value: string): ChannelResponse;
    getType(): ResponseType;
    setType(value: ResponseType): ChannelResponse;
    getMessage(): string;
    setMessage(value: string): ChannelResponse;
    getTimestamp(): number;
    setTimestamp(value: number): ChannelResponse;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): ChannelResponse.AsObject;
    static toObject(includeInstance: boolean, msg: ChannelResponse): ChannelResponse.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: ChannelResponse, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): ChannelResponse;
    static deserializeBinaryFromReader(message: ChannelResponse, reader: jspb.BinaryReader): ChannelResponse;
}

export namespace ChannelResponse {
    export type AsObject = {
        channelId: string,
        type: ResponseType,
        message: string,
        timestamp: number,
    }
}

export enum MessageType {
    MESSAGE_TYPE_UNSPECIFIED = 0,
    MESSAGE_TYPE_JOIN = 1,
    MESSAGE_TYPE_LEAVE = 2,
    MESSAGE_TYPE_CHAT = 3,
    MESSAGE_TYPE_SYNC = 4,
}

export enum ResponseType {
    RESPONSE_TYPE_UNSPECIFIED = 0,
    RESPONSE_TYPE_ACK = 1,
    RESPONSE_TYPE_ERROR = 2,
    RESPONSE_TYPE_SYNC = 3,
}
