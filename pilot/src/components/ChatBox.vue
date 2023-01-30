<template>
    <div id="ChatBox">
        <div class="ChatBox__Input">
            <v-text-field
                hide-details
                prepend-icon="mdi-magnify"
                single-line
            ></v-text-field>
        </div>
        <div class="ChatBox__List">
            <chat-bubble v-for="message in messages" :data="message"></chat-bubble>
        </div>
    </div>
</template>

<script>
    import ChatBubble from './ChatBubble.vue'
    export default {
        components: { ChatBubble },
        data() {
            return {
                messages: [{
                    text: "Hi, how can I help you?",
                    agent: true
                }],
            }
        }
    }
</script>