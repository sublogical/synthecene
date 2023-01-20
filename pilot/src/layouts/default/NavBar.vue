<template>
    <v-navigation-drawer
        v-model="layout_state.drawer"
        app
        clipped
        permanent
      >
      
      <v-list dense nav>
        <v-list-item v-for="item in items" :key="item.title" link :to="item.link">
          <template v-slot:prepend>
            <v-icon>{{ item.icon }}</v-icon>
          </template> 

          <v-list-item-title v-text="item.title"></v-list-item-title>
        </v-list-item>
      </v-list>
    </v-navigation-drawer>
</template>
  
<script>
import { layout_state } from '../../store'
import { ref } from 'vue';

export default {
  name: 'App',
  data: () => ({
    layout_state,
    items: [
      { title: 'Crawler',    icon: 'mdi-weather-windy',  link: '/crawler' },
      { title: 'Tasks',      icon: 'mdi-checkbox-marked-circle-auto-outline', link: '/tasks' },
      { title: 'Models',     icon: 'mdi-flask-outline',  link: '/models' },
      { title: 'Users',      icon: 'mdi-account-outline',    link: '/people' },
      { title: 'Annotation', icon: 'mdi-pencil-outline', link: '/annotation' },
      { title: 'Settings',   icon: 'mdi-cog-outline',    link: '/settings' }
    ],
  }),
}

import { watch} from 'vue'

watch (
    () => layout_state.drawer,
    (val) => {
        console.log('drawer: ' + val);
    }
)
</script>