<template>
  <v-container class="fill-height">
    <v-responsive class="d-flex align-top text-center fill-height">
      <div class="text-body-2 font-weight-light mb-n1">
        <ChatBox />
      </div>
    </v-responsive>
  </v-container>
</template>

<script lang="ts" setup>
  import ChatBox from '@/components/ChatBox.vue'
</script>
