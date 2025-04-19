<template>
  <v-app-bar app clipped-left
    color="indigo-darken-4">
    <v-app-bar-nav-icon @click="layout_state.drawer = !layout_state.drawer"></v-app-bar-nav-icon>
    
    <v-toolbar-title>Calico Pilot</v-toolbar-title>

    <v-btn class="text-none" stacked to="/">
      <v-badge dot color="success">
        <v-icon>mdi-home-outline</v-icon>
      </v-badge>
    </v-btn>

    <v-btn class="text-none" stacked to="People">
      <v-icon>mdi-account-multiple-outline</v-icon>
    </v-btn>

    <v-btn class="text-none" stacked to="Chat">
      <v-badge
        :content="store.chat_count"
        :model-value="store.chat_count > 0"
        color="success"
        overlap
      >
        <v-icon>mdi-chat-outline</v-icon>
      </v-badge>
    </v-btn>

    <v-btn class="text-none" stacked to="Tasks">
      <v-badge
        :content="store.task_count"
        :model-value="store.task_count > 0"
        color="success"
        overlap
      >
        <v-icon>mdi-list-box-outline</v-icon>
      </v-badge>
    </v-btn>

    <v-btn class="text-none" stacked to="Notifications">
      <v-badge
        :content="store.notification_count"
        :model-value="store.notification_count > 0"
        color="success"
        overlap
      >
        <v-icon>mdi-bell-outline</v-icon>
      </v-badge>
    </v-btn>

    <v-text-field
        hide-details
        prepend-icon="mdi-magnify"
        single-line
      ></v-text-field>

    <v-spacer width="10"></v-spacer>
  </v-app-bar>
</template>

<script lang="ts">

import { layout_state } from '../../store'
import { useAppStore } from '@/store/app'


export default {
  setup() {
    const store = useAppStore()

    return {
      // you can return the whole store instance to use it in the template
      store,
      layout_state,
    }
  },
}
</script>
