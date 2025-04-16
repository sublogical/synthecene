var global = globalThis; //<- this should be enough

import { GrpcWebClientBase } from 'grpc-web';
import { GlimmerClient } from '../proto-generated/glimmer_api_grpc_web_pb';
import { 
    NotificationRequest,
    NotificationResponse,
    ContextRequest,
    ContextResponse,
} from '../proto-generated/glimmer_api_pb';

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
        this.client = new GlimmerClient(serverUrl, null, { "useFetchDownloadStream": true });

/**

        this.reportContext("HI", "HI", "HI");
        // Set up notification stream
        const notificationRequest = new NotificationRequest();
        const stream = this.client.getNotifications(notificationRequest);
        
        stream.on('data', (notification: NotificationResponse) => {
            this.emitNotification(notification);
        });

        stream.on('error', (error: Error) => {
            console.error('GRPC stream error:', error);
        });
         */
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
                request, {},
                (error: any | null, response: ContextResponse) => {
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

import XMLHttpRequestPolyfill from 'sw-xhr';

function getData(url: string): Promise<string> {
    return new Promise((resolve, reject) => {
      console.log("loading");
      const xhr = new XMLHttpRequestPolyfill();
      console.log("loaded");
  
      xhr.onload = () => {
        console.log("ONLOAD");
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve(xhr.responseText);
        } else {
          reject(new Error(`Request failed with status ${xhr.status}`));
        }
      };
  
      xhr.onerror = () => {
        reject(new Error('Network error'));
      };
  
      xhr.open('GET', url);
      xhr.send();
    });
}

chrome.runtime.onInstalled.addListener(() => {
    const remote_url = 'localhost:1234';
    console.log(`Mote Starting up, connecting to remote agent at ${remote_url}`);
    const agent = new RemoteAgent(remote_url);

    // NEXT THING TO TRY
    console.log('TRYING TO FETCH');
    fetch('http://localhost:4200/')
        .then(response => console.log("GOOD FETCH"))
        .catch(error => console.error('Fetch error:', error));

    console.log('TRYING TO POLYFILL');
    getData('http://localhost:4200/')
        .then(response => console.log("GOOD POLYFILL"))
        .catch(error => console.error('Fetch error:', error));

    // Add listener
    agent.onNotification.addListener((notification: NotificationResponse) => {
        console.log('Received notification:', notification);
    });

    chrome.webNavigation.onCompleted.addListener(() => {
        console.log("Hit Google");
    }, { url: [{ hostContains: 'google.com' }] });
});
