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

<script>
    import ChatBubble from './ChatBubble.vue'
    export default {
        components: { ChatBubble },
        data() {
            return {
                messages: [
                    {
                        text: "Hi, how can I help you?",
                        agent: true
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'm sorry, we don't have any giraffes available at the moment. Would you like to order a 60' tall giraffe instead?",
                        agent: true
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                    {
                        text: "I'd like to order a 80' tall giraffe. How much is that?",
                        agent: false
                    },
                ],
            }
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