import * as grpc from '@grpc/grpc-js';

import { GlimmerClient } from './proto/glimmer_api_grpc_pb';
import { 
    NotificationRequest,
    NotificationResponse,
    ContextRequest,
    ContextResponse,
} from './proto/glimmer_api_pb';

interface Notification {
    message: string;
    type: string;
    timestamp: number;
}

type NotificationListener = (notification: NotificationResponse) => void;

class EventBroker<T extends Function> {
    private listeners: T[] = [];

    addListener(callback: T): void {
        if (!this.hasListener(callback)) {
            this.listeners.push(callback);
        }
    }

    removeListener(callback: T): void {
        this.listeners = this.listeners.filter(listener => listener !== callback);
    }

    hasListener(callback: T): boolean {
        return this.listeners.includes(callback);
    }

    protected emit(...args: any[]): void {
        this.listeners.forEach(listener => {
            try {
                listener(...args);
            } catch (error) {
                console.error('Error in event listener:', error);
            }
        });
    }
}

class RemoteAgent {
    private client: GlimmerClient;
    public readonly onNotification = new EventBroker<NotificationListener>();

    constructor(serverUrl: string) {
        this.client = new GlimmerClient(
            serverUrl,
            grpc.credentials.createInsecure()  // Use createSsl() for production
        );

        // Set up notification stream
        const notificationRequest = new NotificationRequest();
        const stream = this.client.getNotifications(notificationRequest);
        
        stream.on('data', (notification: NotificationResponse) => {
            this.emitNotification(notification);
        });

        stream.on('error', (error: Error) => {
            console.error('GRPC stream error:', error);
        });
    }

    protected emitNotification(notification: NotificationResponse): void {
        (this.onNotification as any).emit(notification);
    }

    async reportContext(url: string, title: string, content: string): Promise<ContextResponse> {
        const request = new ContextRequest();
        request.setUrl(url);
        request.setTimestamp(Date.now());
        request.setTitle(title);
        request.setContent(content);

        return new Promise((resolve, reject) => {
            this.client.reportContext(
                request,
                (error: grpc.ServiceError | null, response: ContextResponse) => {
                    if (error) {
                        reject(error);
                    } else {
                        resolve(response);
                    }
                }
            );
        });
    }
}

chrome.runtime.onInstalled.addListener(() => {
    const agent = new RemoteAgent('localhost:1234');

    console.log("Mote Installed");

    // Add listener
    agent.onNotification.addListener((notification: NotificationResponse) => {
        console.log('Received notification:', notification);
    });

    chrome.webNavigation.onCompleted.addListener(() => {
        console.log("Hit Google");
    }, { url: [{ hostContains: 'google.com' }] });
});
