<template>
    <div v-bind:class="{ agent: isAgent, user: isUser }" class="chat-bubble">
        <div class="chat-bubble__content"
        >
            {{ data.text }}
        </div>
    </div>
</template>

<script lang="ts">
    export default {
        props: ['data'],
        computed: {
            isAgent() {
                return this.data.agent == true
            },
            isUser() {
                return this.data.agent == false
            }
        }
    }
</script>

<style>
    .chat-bubble {
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        margin: 0.5rem 0;
    }
    .chat-bubble.agent {
    }

    .chat-bubble.user {
    }
    .chat-bubble__content {
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        margin: 0.5rem 0;
        padding: 1rem 1rem;
        border-radius: 1rem;
        background-color: #202080;
    }
    .chat-bubble.agent .chat-bubble__content {
        align-items: flex-end;
        background-color: #202020;
    }
    .chat-bubble__content p {
        margin: 0;
    }
</style>