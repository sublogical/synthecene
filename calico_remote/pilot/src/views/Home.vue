<template>
  <v-container class="fill-height">
    <v-responsive class="d-flex align-center text-center fill-height">
      <div class="text-body-2 font-weight-light mb-n1">
        <p v-if="loading">
         Still loading..
        </p>
        <p v-else>
          DATA
          {{ JSON.stringify(data) }}
          <div>{{ data[0]["message"] }}</div>
        </p>
        <p v-if="error">
          Error: {{ error }}
        </p>
      </div>
    </v-responsive>
  </v-container>asdasdasd
</template>

<script lang="ts">
import { layout_state } from '../store';
import { ref, computed, onMounted } from "vue";
import type { Ref } from 'vue'

export default {
  name: 'Posts',
  props: {
  },
  setup() {
    const currentUrl = location.toString();
    const hostname = location.hostname;

    const data: Ref<any | null> = ref(null);
    const loading = ref(true);
    const error: Ref<Error | null> = ref(null);

    const api_port = 3030;
    const chat_id = 1;

    const chat_url = `http://${hostname}:${api_port}/chat/${chat_id}/message`
    function fetchData() {
      loading.value = true;

      console.log("starting fetch to " + chat_url);
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
            data.value = JSON.parse(str);
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
      data,
      loading,
      error
    };
  }
}
</script>
