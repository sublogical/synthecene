import type { Configuration } from 'webpack';
import webpack from 'webpack';

export default {
  entry: { 
    background: { 
      import: ['process/browser', 'src/background.ts'], 
      runtime: false 
    }
  },
  optimization: {
    minimize: false,
  },
  resolve: {
    fallback: {
      "stream": require.resolve("stream-browserify"),
      "util": require.resolve("util/"),
      "zlib": require.resolve("browserify-zlib"),
      "http2": false,
      "path": require.resolve("path-browserify"),
      "assert": require.resolve("assert/"),
      "os": require.resolve("os-browserify/browser"),
      "dns": false,
      "net": false,
      "tls": false,
      "fs": false,
      "url": require.resolve("url/"),
      "http": require.resolve("stream-http")
    }
  },
  plugins: [
    new webpack.ProvidePlugin({
      "process": 'process/browser',
    }),
  ],
} as Configuration;