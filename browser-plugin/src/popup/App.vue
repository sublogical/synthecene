<template>
  <div class="extension">
    <kicker>Tips & Tricks</kicker>

    <h2>Search on MyNotePapar</h2>

    <input v-model="keyword" placeholder="Enter keyword">
    <button v-on:click="buttonSearch">Search</button>

    <p>To get more articles visit <a target="_blank" href="https://shouts.dev/" style="text-decoration: none;">mynotepaper.com</a></p>
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
      alert("Search on MyNotePaper:" + this.keyword);
    }
  }
};
</script>

<style lang="scss" scoped>
p {
  font-size: 16px;
}
.extension {
  width: 800px;
  text-align: center;
}

.card-display {
  @apply bg-white inline-flex flex-col shadow rounded-xl;
}
</style>
