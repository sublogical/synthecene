<template>
    <div id="ChatBox">
        <div class="ChatBox__List">
            <chat-bubble v-for="message in messages" :data="message"></chat-bubble>
        </div>
    </div>

    <div id="Footer">
        <div class="ChatBox__Input">
            <v-text-field
                hide-details
                prepend-icon="mdi-magnify"
                single-line
            ></v-text-field>
        </div>
    </div>
</template>

<script lang="ts">
    import axios from 'axios';

    interface Message {
        chat_id: string,
        message_id: string,
        agent: boolean,
        message: string
    }

    const hostname = location.hostname;
    const api_port = 3030;

    function GetMessages(chat_id: string): Promise<Array<Message>> {
        return axios.get<Array<Message>>(`http://${hostname}:${api_port}/chat/${chat_id}/message`)
            .then((response: { data: Message; }) => {
                console.log("GET Response")
                console.log(response.data);
                
                return response.data;
            });
    }

    function PostMessage(chat_id: string, message: string) {
        return axios.post(`http://${hostname}:${api_port}/chat/${chat_id}/message`, {
            message: message
        })
            .then((response: { data: Message; }) => {
                console.log("POST Response")
                console.log(response.data);
                
                return response.data;
            });
    }


    import ChatBubble from './ChatBubble.vue'
    import { ref, computed, onMounted } from "vue";
    import type { Ref } from 'vue'

    export default {
        components: { ChatBubble },
        setup() {
            const currentUrl = location.toString();
            const hostname = location.hostname;

            const messages: Ref<any | null> = ref(null);
            const loading = ref(true);
            const error: Ref<Error | null> = ref(null);

            const api_port = 3030;
            const chat_id = "1";

            const chat_url = `http://${hostname}:${api_port}/chat/${chat_id}/message`
            function fetchData() {
                loading.value = true;

                GetMessages(chat_id)
                    .then((data: Array<Message>) => {
                        console.log("got data " + data);
                        messages.value = data;
                        loading.value = false;
                    })
                    .catch((error: any) => {
                        error.value = error;
                    });
            }

            onMounted(() => {
                fetchData();
            });

            return {
                hostname,
                messages,
                loading,
                error
            };
        }
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