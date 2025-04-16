import type { Configuration } from 'webpack';
import webpack from 'webpack';
import NodePolyfillPlugin from 'node-polyfill-webpack-plugin';

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
  devtool: 'inline-source-map',
  resolve: {
    fallback: {
      XMLHttpRequest: require.resolve("sw-xhr"),
    }
  },
  target: 'web',
} as Configuration;