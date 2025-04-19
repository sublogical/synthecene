<template>
    <div id="ChatBox">
        <div class="ChatBox__List">
            <chat-bubble v-for="message in messages" :data="message"></chat-bubble>
        </div>
    </div>

    <div>
        {{ messages.length }}
    </div>
    <div id="Footer">
        <div class="ChatBox__Input">
            <v-text-field
                hide-details
                prepend-icon="mdi-magnify"
                single-line
                @keyup.enter="PostMessage"
            ></v-text-field>
        </div>
    </div>
</template>

<script lang="ts">
    import axios from 'axios';

    const hostname = location.hostname;
    const api_port = 3030;

    interface Message {
        chat_id: string,
        message_id: string,
        agent: boolean,
        message: string
    }

    interface PostMessage {
        agent: boolean,
        message: string
    }

    function GetMessages(chat_id: string): Promise<Array<Message>> {
        return axios.get<Array<Message>>(`http://${hostname}:${api_port}/chat/${chat_id}/message`)
            .then((response: { data: Array<Message>; }) => {
                console.log("GET Response")
                console.log(response.data);
                
                return response.data;
            });
    }

    function PostMessage(chat_id: string, message: string) {
        let post_message: PostMessage = {
            agent: false,
            message: message
        }
        return axios.post(`http://${hostname}:${api_port}/chat/${chat_id}/message`, post_message)
            .then((response: { data: Message; }) => {
                console.log("POST Response")
                console.log(response.data);                
            });
    }


    import ChatBubble from './ChatBubble.vue'
    import { ref, computed, onMounted } from "vue";
    import type { Ref } from 'vue'

    export default {
        components: { ChatBubble },
        methods: {
            PostMessage: function (event: KeyboardEvent) {
                if (event.target != null) {
                    let html_target = event.target as HTMLInputElement;
                    console.log("posting new message: " + html_target.value);
                    PostMessage(this.chat_id, html_target.value)
                        .then(() => {
                            this.RefreshChat();
                        })
                }
            },
            RefreshChat: function () {
                console.log("refreshing chat");

                this.loading = true;

                GetMessages(this.chat_id)
                    .then((data: Array<Message>) => {
                        console.log("Number of Messages: " + data.length);
                        this.messages = data;
                        this.loading = false;
                    })
                    .catch((error: any) => {
                        error.value = error;
                    });

            }
        },
        data() {
            const messages:Array<Message> = [];
            const loading = ref(true);
            const error: Ref<Error | null> = ref(null);
            const chat_id = "1";

            return {
                messages,
                loading,
                error,
                chat_id
            };
        },
        mounted() {
            this.RefreshChat();
        },
    }
</script>

<style>
#ChatBox {
    display: flex;
    flex-direction: column;
    width: 800px;
    height: 100%;
}
#Footer{
 position:fixed;
 bottom:0;
 width: 800px;
 z-index: 100;  
 opacity: 1;
 background-color: rgb(var(--v-primary-base));
}
</style>