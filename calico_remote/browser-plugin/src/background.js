//browser.runtime.onMessage.addListener(function (request, sender, sendResponse) {
browser.runtime.onMessage.addListener(function () {
  console.log('Hello from the background')

  browser.tabs.executeScript({
    file: 'content-script.js',
  });
})


global.browser = require('webextension-polyfill')
