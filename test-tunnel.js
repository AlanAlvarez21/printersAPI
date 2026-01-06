// Simple test to verify tunnel functionality
const { startNgrok } = require('./tunnel');

console.log('Testing tunnel functionality...');

startNgrok(3000)
  .then(tunnelUrl => {
    console.log('Tunnel started successfully:', tunnelUrl);
  })
  .catch(error => {
    console.error('Tunnel failed:', error);
  });