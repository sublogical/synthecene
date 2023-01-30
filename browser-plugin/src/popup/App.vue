<template>
  <div class="extension">
    <div class="agent">
      <div class="card-display">How can I help?</div>
    </div>  

    <v-divider></v-divider>

    <input v-model="keyword" placeholder="Enter keyword">
    <button v-on:click="buttonSearch">Search</button>
  </div>
</template>

<script>
//const browser = require("webextension-polyfill");
export default {
  data() {
    return {
      keyword: "",
    };
  },
  methods: {
    buttonSearch() {
      alert("Entered Dialog:" + this.keyword);
    }
  }
};
</script>

<style lang="scss" scoped>
p {
  font-size: 16px;
}
.extension {
  width: 400px;
  text-align: center;
  @apply bg-slate-400;
}

.agent {
  text-align: left;
}

.card-display {
  width: 300px;
  height: 30px;
  margin: 10px;
  padding: 4px;

  @apply bg-slate-50 inline-flex flex-col shadow rounded-md;
}
</style>
