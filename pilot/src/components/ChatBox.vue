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
            const chat_id = 1;

            const chat_url = `http://${hostname}:${api_port}/chat/${chat_id}/message`
            function fetchData() {
                loading.value = true;

                return fetch(chat_url, {
                    method: 'GET',
                    headers: {
                    'content-type': 'application/json'
                    }
                }).then(res => {
                    console.log("got result " + res.status);
                    if (!res.ok) {
                    error.value = new Error(res.statusText);;
                    } else {
                    res.text().then((str) => {
                        console.log("got data " + str);
                        messages.value = JSON.parse(str);
                        loading.value = false;
                    });
                    }
                })
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