<template>
  <div id="app">
    <button @click="greet">Greet me</button>
  </div>
</template>

<script>

export default {
  name: 'App',
  methods: {
    async greet() {
      const wasm = import("../../pkg");
      const greet = (await wasm).greet;
      greet("hi");
    }
  }
}
</script>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>
